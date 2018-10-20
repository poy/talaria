package raft_test

import (
	"testing"

	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
	"github.com/poy/talaria/node/internal/raft"
	"github.com/poy/talaria/node/internal/raft/buffers/ringbuffer"
	rafthashi "github.com/hashicorp/raft"
)

type TFSM struct {
	*testing.T
	fsm    rafthashi.FSM
	buffer *ringbuffer.RingBuffer
}

func TestFSM(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TFSM {
		buffer := ringbuffer.New(5)
		return TFSM{
			T:      t,
			fsm:    raft.NewFSM(buffer),
			buffer: buffer,
		}
	})

	o.Group("Apply", func() {
		o.Spec("it writes the log to the buffer", func(t TFSM) {
			l := &rafthashi.Log{Data: []byte("some-data")}
			t.fsm.Apply(l)

			entry, _, _ := t.buffer.ReadAt(0)
			Expect(t, entry).To(Equal(l))
		})
	})
}
