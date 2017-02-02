package raft_test

import (
	"testing"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/api/stored"
	"github.com/apoydence/talaria/node/internal/raft"
	"github.com/apoydence/talaria/node/internal/raft/buffers/ringbuffer"
	"github.com/golang/protobuf/proto"
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
			result := t.fsm.Apply(l)

			entry, _, _ := t.buffer.ReadAt(0)
			Expect(t, entry).To(Equal(l))
			Expect(t, result).To(BeTrue())
		})

		o.Spec("it returns false and rejects data after read only message", func(t TFSM) {
			readOnly := &stored.Data{
				Type: stored.Data_ReadOnly,
			}
			data, err := proto.Marshal(readOnly)
			Expect(t, err == nil).To(BeTrue())
			l := &rafthashi.Log{Data: data}

			result := t.fsm.Apply(l)
			Expect(t, result).To(BeTrue())

			normal := &stored.Data{
				Type:    stored.Data_Normal,
				Payload: []byte("some-data"),
			}
			data, err = proto.Marshal(normal)
			Expect(t, err == nil).To(BeTrue())

			result = t.fsm.Apply(l)
			Expect(t, result).To(BeFalse())

			_, _, err = t.buffer.ReadAt(1)
			Expect(t, result).To(BeFalse())
		})
	})
}
