package raft

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/apoydence/talaria/api/stored"
	"github.com/apoydence/talaria/node/internal/raft/buffers/ringbuffer"
	"github.com/apoydence/talaria/node/internal/raft/network"
	"github.com/golang/protobuf/proto"
	rafthashi "github.com/hashicorp/raft"
)

type BufferReader interface {
	ReadAt(readIndex uint64) (*rafthashi.Log, uint64, error)
	LastIndex() uint64
}

type ConsumerFetcher interface {
	Fetch(bufferName string) <-chan rafthashi.RPC
	Addr() string
}

type Raft struct {
	node   *rafthashi.Raft
	buffer BufferReader
	repair *PeerRepair
}

func Build(bufferName string, consumerFetcher ConsumerFetcher, ops ...BuildOp) (*Raft, error) {
	config := Config{
		Config:     rafthashi.DefaultConfig(),
		bufferSize: 100,
	}
	config.ShutdownOnRemove = false

	for _, op := range ops {
		op(&config)
	}

	if len(config.peers) == 0 {
		log.Printf("Buffer %s does not have any peers. Enabling Single Node mode", bufferName)
		config.peers = []string{consumerFetcher.Addr()}
		config.EnableSingleNode = true
	}

	clientCache := network.NewClientCache()
	buffer := ringbuffer.New(config.bufferSize)
	fsm := NewFSM(buffer)
	logStore := rafthashi.NewInmemStore()
	snapStore := NewSnapshotStore()
	consumer := consumerFetcher.Fetch(bufferName)
	transport := NewTransport(bufferName, consumerFetcher.Addr(), clientCache, consumer)
	peerStore := &rafthashi.StaticPeers{StaticPeers: config.peers}

	node, err := rafthashi.NewRaft(
		config.Config,
		fsm,
		logStore,
		logStore,
		snapStore,
		peerStore,
		transport,
	)

	repair := NewPeerRepair(peerStore, node)

	if err != nil {
		return nil, err
	}

	return &Raft{
		node:   node,
		buffer: buffer,
		repair: repair,
	}, nil
}

func (r *Raft) Write(storedData stored.Data, timeout time.Duration) error {
	data, err := proto.Marshal(&storedData)
	if err != nil {
		return err
	}

	fut := r.node.Apply(data, timeout)
	if err := fut.Error(); err != nil {
		return err
	}

	if result, ok := fut.Response().(bool); ok && !result {
		return fmt.Errorf("buffer set to ReadOnly")
	}

	return nil
}

func (r *Raft) ReadAt(index uint64) (entry []byte, seq uint64, err error) {
	logEntry, seq, err := r.buffer.ReadAt(index)

	if err != nil {
		return nil, 0, err
	}

	if logEntry == nil {
		return nil, seq, nil
	}

	storedData := new(stored.Data)
	if err := proto.Unmarshal(logEntry.Data, storedData); err != nil {
		return nil, 0, err
	}

	if storedData.Type == stored.Data_ReadOnly {
		return nil, 0, io.EOF
	}

	return storedData.Payload, seq, nil
}

func (r *Raft) LastIndex() uint64 {
	return r.buffer.LastIndex()
}

func (r *Raft) Leader() string {
	return r.node.Leader()
}

func (r *Raft) SetExpectedPeers(peers []string) {
	r.repair.SetExpectedPeers(peers)
}

func (r *Raft) ExpectedPeers() (peers []string) {
	return r.repair.ExpectedPeers()
}

func (r *Raft) Shutdown() rafthashi.Future {
	return r.node.Shutdown()
}