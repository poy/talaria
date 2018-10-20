package raft

import (
	"fmt"
	"io"
	"log"

	"github.com/poy/talaria/node/internal/raft/buffers/ringbuffer"
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

	return nil, fmt.Errorf("not implemented")
}

func (f *fsm) Restore(io.ReadCloser) error {
	log.Println("Restoring from snapshot...")
	defer log.Println("Done restoring from snapshot.")

	return fmt.Errorf("not implemented")
}
