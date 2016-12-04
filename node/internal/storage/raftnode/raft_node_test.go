//go:generate hel
package raftnode_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/node/internal/storage/buffers/ringbuffer"
	"github.com/apoydence/talaria/node/internal/storage/raftnode"
	"github.com/coreos/etcd/raft"
)

type TR struct {
	*testing.T
	storage     *raftnode.State
	node        *raftnode.RaftNode
	mockNetwork *mockNetwork
}

func TestRaftNodeStart(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TR {
		storage := raftnode.NewState(ringbuffer.New(50))
		mockNetwork := newMockNetwork()

		return TR{
			T:           t,
			storage:     storage,
			node:        raftnode.Start(99, storage, mockNetwork, nil),
			mockNetwork: mockNetwork,
		}
	})

	o.Spec("sets the HardState on the storage", func(t TR) {
		f := func() bool {
			h, _, _ := t.storage.InitialState()
			return !raft.IsEmptyHardState(h)
		}
		Expect(t, f).To(ViaPolling(BeTrue()))
	})

	o.Spec("stores data in storage", func(t TR) {
		for i := 0; i < 5; i++ {
			err := t.node.Propose(context.Background(), []byte(fmt.Sprintf("some-data-%d", i)))
			Expect(t, err == nil).To(BeTrue())
		}

		var next uint64
		f := func() string {
			data, seq, err := t.storage.Buffer.ReadAt(next)
			if err != nil {
				return ""
			}

			next = seq + 1
			return string(data.Data)
		}
		for i := 0; i < 5; i++ {
			Expect(t, f).To(ViaPolling(Equal(fmt.Sprintf("some-data-%d", i))))
			Expect(t, t.mockNetwork.EmitInput.Msgs).To(ViaPolling(Not(HaveLen(0))))
		}
	})
}
