package storage

import (
	"context"
	"fmt"
	"log"

	"github.com/apoydence/talaria/node/internal/server"
	"github.com/apoydence/talaria/node/internal/storage/buffers/ringbuffer"
	"github.com/apoydence/talaria/node/internal/storage/intraserver/router"
	"github.com/apoydence/talaria/node/internal/storage/raftnode"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/coreos/etcd/raft/raftpb"
)

type Receiver interface {
	Receiver(bufferName string) func() (raftpb.Message, error)
}

type URIFinder interface {
	FromID(ID uint64) (string, error)
}

type Storage struct {
	bufs      map[string]raftNodeInfo
	id        uint64
	receiver  Receiver
	uriFinder URIFinder
}

func New(ID uint64, receiver Receiver, uriFinder URIFinder) *Storage {
	return &Storage{
		bufs:      make(map[string]raftNodeInfo),
		id:        ID,
		receiver:  receiver,
		uriFinder: uriFinder,
	}
}

type raftNodeInfo struct {
	node *raftnode.RaftNode
}

func (s *Storage) Create(name string, peers []*intra.PeerInfo) error {
	log.Printf("Creating '%s'", name)
	if _, ok := s.bufs[name]; ok {
		log.Printf("'%s' already exists...", name)
		return nil
	}

	emitter := router.NewEmitter(name, s.uriFinder)
	storage := raftnode.NewState(ringbuffer.New(100))
	network := raftnode.NewNetworkWrapper(name, s.receiver, emitter)
	s.bufs[name] = raftNodeInfo{
		node: raftnode.Start(s.id, storage, network, peers),
	}
	return nil
}

func (s *Storage) FetchWriter(name string) (server.Writer, error) {
	info, ok := s.bufs[name]
	if !ok {
		return nil, fmt.Errorf("'%s' must be created before being fetched", name)
	}

	return info.node, nil
}

func (s *Storage) FetchReader(name string) (server.Reader, error) {
	info, ok := s.bufs[name]
	if !ok {
		return nil, fmt.Errorf("'%s' must be created before being fetched", name)
	}

	return info.node, nil
}

func (s *Storage) Leader(name string) (id uint64, err error) {
	info, ok := s.bufs[name]
	if !ok {
		return 0, fmt.Errorf("unknown buffer: %s", name)
	}
	return info.node.Leader()
}

func (s *Storage) UpdateConfig(name string, conf raftpb.ConfChange) error {
	info, ok := s.bufs[name]
	if !ok {
		return fmt.Errorf("unknown buffer: %s", name)
	}
	return info.node.PropseConfChange(context.Background(), conf)
}

func (s *Storage) List() []string {
	var buffers []string
	for name, _ := range s.bufs {
		buffers = append(buffers, name)
	}
	return buffers
}
