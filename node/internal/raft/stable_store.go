package raft

import (
	"log"
	"sync"

	rafthashi "github.com/hashicorp/raft"
)

type stableStore struct {
	lock sync.RWMutex

	mb map[string][]byte
	mi map[string]uint64
}

func NewStableStore() rafthashi.StableStore {
	return &stableStore{
		mb: make(map[string][]byte),
		mi: make(map[string]uint64),
	}
}

func (s *stableStore) Set(key []byte, val []byte) error {
	log.Printf("Setting key (key=%s val=%v)...", string(key), val)
	defer log.Printf("Done setting key (key=%s val=%v).", string(key), val)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.mb[string(key)] = val

	return nil
}

func (s *stableStore) Get(key []byte) ([]byte, error) {
	log.Printf("Getting key (key=%s)...", string(key))
	defer log.Printf("Done getting key (key=%s).", string(key))

	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.mb[string(key)], nil
}

func (s *stableStore) SetUint64(key []byte, val uint64) error {
	log.Printf("SetUint64 value (key=%s val=%d)...", string(key), val)
	defer log.Printf("Done SetUint64 value (key=%s val=%d).", string(key), val)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.mi[string(key)] = val
	return nil
}

func (s *stableStore) GetUint64(key []byte) (uint64, error) {
	log.Printf("GetUint64 value (key=%s)...", string(key))
	defer log.Printf("Done GetUint64 value (key=%s).", string(key))

	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.mi[string(key)], nil
}
