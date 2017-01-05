package raft

import (
	"io"
	"log"

	rafthashi "github.com/hashicorp/raft"
)

type snapshotStore struct {
}

func NewSnapshotStore() rafthashi.SnapshotStore {
	return new(snapshotStore)
}

func (s *snapshotStore) Create(index, term uint64, peers []byte) (rafthashi.SnapshotSink, error) {
	log.Printf("Creating snapshot (index=%d term=%d)...", index, term)
	defer log.Printf("Creating snapshot (index=%d term=%d).", index, term)

	return nil, nil
}

func (s *snapshotStore) List() ([]*rafthashi.SnapshotMeta, error) {
	log.Println("Requesting Snapshot List...")
	defer log.Println("Done requesting Snapshot List.")

	return nil, nil
}

func (s *snapshotStore) Open(id string) (*rafthashi.SnapshotMeta, io.ReadCloser, error) {
	log.Printf("Opening snapshot (id=%s)...", id)
	defer log.Printf("Done opening snapshot (id=%s).", id)

	return nil, nil, nil
}
