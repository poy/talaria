package iofetcher

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/apoydence/talaria/node/internal/server"
	"github.com/apoydence/talaria/pb/stored"
)

var (
	BufferNotCreated     = errors.New("buffer not created")
	BufferAlreadyCreated = errors.New("buffer alreadycreated")
)

type RaftCluster interface {
	Write(data stored.Data, timeout time.Duration) error
	ReadAt(index uint64) ([]byte, uint64, error)
	LastIndex() uint64
	Leader() string

	SetExpectedPeers(peers []string)
	ExpectedPeers() []string
}

type RaftClusterCreator func(name string, peers []string) (RaftCluster, error)

type IOFetcher struct {
	creator RaftClusterCreator

	mu    sync.Mutex
	rafts map[string]RaftCluster
}

func New(creator RaftClusterCreator) *IOFetcher {
	return &IOFetcher{
		creator: creator,
		rafts:   make(map[string]RaftCluster),
	}
}

func (f *IOFetcher) Create(name string, peers []string) error {
	r, err := f.creator(name, peers)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rafts[name]; ok {
		return BufferAlreadyCreated
	}

	f.rafts[name] = r
	return nil
}

func (f *IOFetcher) ReadOnly(name string) error {
	r := f.fetchRaft(name)
	if r == nil {
		return BufferNotCreated
	}

	if err := r.Write(stored.Data{Type: stored.Data_ReadOnly}, time.Second); err != nil {
		log.Printf("Failed to set buffer %s to ReadOnly: %s", name, err)
		return err
	}

	return nil
}

func (f *IOFetcher) FetchWriter(name string) (server.Writer, error) {
	r := f.fetchRaft(name)
	if r == nil {
		return nil, BufferNotCreated
	}

	return r, nil
}

func (f *IOFetcher) FetchReader(name string) (server.Reader, error) {
	r := f.fetchRaft(name)
	if r == nil {
		return nil, BufferNotCreated
	}

	return r, nil
}

func (f *IOFetcher) Leader(name string) (string, error) {
	r := f.fetchRaft(name)
	if r == nil {
		return "", BufferNotCreated
	}

	return r.Leader(), nil
}

func (f *IOFetcher) SetExpectedPeers(name string, peers []string) error {
	r := f.fetchRaft(name)
	if r == nil {
		return BufferNotCreated
	}

	r.SetExpectedPeers(peers)
	return nil
}

func (f *IOFetcher) Status() map[string][]string {
	m := make(map[string][]string)
	for name, cluster := range f.rafts {
		m[name] = cluster.ExpectedPeers()
	}

	return m
}

func (f *IOFetcher) fetchRaft(name string) RaftCluster {
	if r, ok := f.rafts[name]; ok {
		return r
	}

	return nil
}
