package raft

import (
	"io"
	"log"

	"github.com/apoydence/talaria/node/internal/raft/buffers/ringbuffer"
	rafthashi "github.com/hashicorp/raft"
)

type fsm struct {
	buffer *ringbuffer.RingBuffer
}

func NewFSM(buffer *ringbuffer.RingBuffer) rafthashi.FSM {
	return &fsm{
		buffer: buffer,
	}
}

func (f *fsm) Apply(entry *rafthashi.Log) interface{} {
	f.buffer.Write(entry)

	return nil
}

func (f *fsm) Snapshot() (rafthashi.FSMSnapshot, error) {
	log.Println("Requesting Snapshot...")
	defer log.Println("Done requesting Snapshot.")

	return nil, nil
}

func (f *fsm) Restore(io.ReadCloser) error {
	log.Println("Restoring from snapshot...")
	defer log.Println("Done restoring from snapshot.")

	return nil
}
