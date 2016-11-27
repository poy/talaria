package storage

import (
	"fmt"
	"log"

	"github.com/apoydence/talaria/node/internal/server"
	"github.com/apoydence/talaria/node/internal/storage/buffers/ringbuffer"
	"github.com/apoydence/talaria/node/internal/storage/raftnode"
)

type Storage struct {
	bufs map[string]*raftnode.RaftNode
}

func New() *Storage {
	return &Storage{
		bufs: make(map[string]*raftnode.RaftNode),
	}
}

func (f *Storage) Create(name string) error {
	log.Printf("Creating '%s'", name)
	if _, ok := f.bufs[name]; ok {
		log.Printf("'%s' already exists...", name)
		return nil
	}

	f.bufs[name] = raftnode.Start(raftnode.NewState(ringbuffer.New(100)))
	return nil
}

func (f *Storage) FetchWriter(name string) (server.Writer, error) {
	writer, ok := f.bufs[name]
	if !ok {
		return nil, fmt.Errorf("'%s' must be created before being fetched", name)
	}

	return writer, nil
}

func (f *Storage) FetchReader(name string) (server.Reader, error) {
	node, ok := f.bufs[name]
	if !ok {
		return nil, fmt.Errorf("'%s' must be created before being fetched", name)
	}

	return node, nil
}
