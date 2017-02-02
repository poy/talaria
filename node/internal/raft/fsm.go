package raft

import (
	"fmt"
	"io"
	"log"
	"sync/atomic"

	"github.com/apoydence/talaria/node/internal/raft/buffers/ringbuffer"
	"github.com/apoydence/talaria/pb/stored"
	"github.com/golang/protobuf/proto"
	rafthashi "github.com/hashicorp/raft"
)

type fsm struct {
	buffer   *ringbuffer.RingBuffer
	readOnly int32
}

func NewFSM(buffer *ringbuffer.RingBuffer) rafthashi.FSM {
	return &fsm{
		buffer: buffer,
	}
}

func (f *fsm) Apply(entry *rafthashi.Log) interface{} {
	if !f.validateData(entry.Data) {
		return false
	}

	f.buffer.Write(entry)

	return true
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

func (f *fsm) validateData(data []byte) (accepted bool) {
	storedData := new(stored.Data)
	if err := proto.Unmarshal(data, storedData); err != nil {
		return true
	}

	if atomic.LoadInt32(&f.readOnly) != 0 {
		return false
	}

	if storedData.Type == stored.Data_ReadOnly {
		return atomic.CompareAndSwapInt32(&f.readOnly, 0, 1)
	}

	return true
}
