package raftnode

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb/intra"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

var (
	ErrMetaData = errors.New("Meta data")
)

type Storage interface {
	raft.Storage
	HardState(raftpb.HardState)
	Write(data raftpb.Entry) (uint64, error)
	ReadAt(index uint64) (raftpb.Entry, uint64, error)
	ConfState(c raftpb.ConfState)
}

type Network interface {
	Recv() (raftpb.Message, error)
	Emit(msgs []raftpb.Message)
}

type RaftNode struct {
	storage    Storage
	node       raft.Node
	bufferName string
	leaderId   uint64
	network    Network
	networkRx  chan raftpb.Message

	mu  sync.RWMutex
	ids []uint64
}

func Start(bufferName string, ID uint64, storage Storage, network Network, peers []*intra.PeerInfo) *RaftNode {
	c := &raft.Config{
		ID:              ID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          newLogger(log.New(os.Stderr, "[RAFT]", log.LstdFlags)),
	}

	ps := append(convertPeers(peers), raft.Peer{ID: ID})
	rn := raft.StartNode(c, ps)

	// TODO: Figure out why we HAVE to have a dummmy entry
	storage.Write(raftpb.Entry{})

	node := &RaftNode{
		bufferName: bufferName,
		storage:    storage,
		node:       rn,
		network:    network,
		networkRx:  make(chan raftpb.Message),
	}

	go node.run()
	go node.readNetwork()

	return node
}

func (r *RaftNode) Propose(ctx context.Context, data []byte) error {
	d := make([]byte, len(data))
	copy(d, data)
	return r.node.Propose(ctx, d)
}

func (r *RaftNode) PropseConfChange(ctx context.Context, cc raftpb.ConfChange) error {
	return r.node.ProposeConfChange(ctx, cc)
}

func (r *RaftNode) ReadAt(index uint64) ([]byte, uint64, error) {
	entry, seq, err := r.storage.ReadAt(index)
	if err != nil {
		return nil, 0, err
	}

	if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
		return entry.Data, seq, ErrMetaData
	}

	return entry.Data, seq, nil
}

func (r *RaftNode) LastIndex() uint64 {
	idx, err := r.storage.LastIndex()
	if err != nil {
		log.Panic(err)
	}

	return idx
}

func (r *RaftNode) Leader() (uint64, error) {
	state := r.node.Status().SoftState
	l := atomic.LoadUint64(&state.Lead)
	if l == 0 {
		return 0, fmt.Errorf("no leader set")
	}

	return l, nil
}

func (r *RaftNode) IDs() []uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ids
}

func (r *RaftNode) run() {
	ticker := time.NewTicker(100 * time.Millisecond).C
	for {
		select {
		case <-ticker:
			r.node.Tick()
		case rd := <-r.node.Ready():
			if !raft.IsEmptyHardState(rd.HardState) {
				r.storage.HardState(rd.HardState)
			}
			for _, entry := range rd.Entries {
				r.storage.Write(entry)
			}

			r.network.Emit(rd.Messages)

			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					log.Printf("Applying Config Update to %s: %v", r.bufferName, cc)
					state := r.node.ApplyConfChange(cc)
					if state != nil {
						r.storage.ConfState(*state)
					}

					r.mu.Lock()
					r.ids = state.Nodes
					r.mu.Unlock()
					log.Printf("Done applying Config Update to %s: %v", r.bufferName, state)
				}
			}

			r.node.Advance()

		case m := <-r.networkRx:
			go r.node.Step(context.Background(), m)
		}
	}
}

func (r *RaftNode) readNetwork() {
	for {
		msg, err := r.network.Recv()
		if err != nil {
			log.Printf("error reading from network: %s", err)
			return
		}
		r.networkRx <- msg
	}
}

func convertPeers(ps []*intra.PeerInfo) []raft.Peer {
	var peers []raft.Peer
	for _, p := range ps {
		peers = append(peers, raft.Peer{ID: p.Id})
	}

	return peers
}
