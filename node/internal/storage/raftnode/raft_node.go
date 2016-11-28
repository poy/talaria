package raftnode

import (
	"errors"
	"time"

	"golang.org/x/net/context"

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
}

type RaftNode struct {
	storage Storage
	node    raft.Node
}

func Start(storage Storage) *RaftNode {
	c := &raft.Config{
		ID:              0x1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          newLogger(),
	}

	rn := raft.StartNode(c, []raft.Peer{{ID: 0x1}})

	// TODO: Figure out why we HAVE to have a dummmy entry
	storage.Write(raftpb.Entry{})

	node := &RaftNode{
		storage: storage,
		node:    rn,
	}

	go node.run()

	return node
}

func (r *RaftNode) Propose(ctx context.Context, data []byte) error {
	d := make([]byte, len(data))
	copy(d, data)
	return r.node.Propose(ctx, d)
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

func (r *RaftNode) LastIndex() uint64 { return 0 }

func (r *RaftNode) run() {
	ticker := time.NewTicker(5 * time.Millisecond).C
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
			r.node.Advance()
		}
	}
}
