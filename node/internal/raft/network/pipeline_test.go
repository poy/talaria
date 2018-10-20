package network_test

import (
	"testing"

	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
	"github.com/poy/talaria/api/intra"
	"github.com/poy/talaria/node/internal/raft/network"
	rafthashi "github.com/hashicorp/raft"
)

type TP struct {
	*testing.T
	pipeline           *network.Pipeline
	mockNodeRaftClient *mockNodeRaftClient
}

func TestPipeline(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TP {
		mockNodeRaftClient := newMockNodeRaftClient()

		close(mockNodeRaftClient.AppendEntriesOutput.Ret1)

		return TP{
			T:                  t,
			pipeline:           network.NewPipeline("some-buffer", "some-target", mockNodeRaftClient),
			mockNodeRaftClient: mockNodeRaftClient,
		}
	})

	o.Group("AppendEntries", func() {
		o.Spec("it writes to the client", func(t TP) {
			args := &rafthashi.AppendEntriesRequest{
				Leader: []byte("some-leader"),
			}

			//			futs := make(chan rafthashi.AppendFuture, 10)

			//			go func() {
			resp := new(rafthashi.AppendEntriesResponse)
			fut, err := t.pipeline.AppendEntries(args, resp)
			Expect(t, err == nil).To(BeTrue())
			Expect(t, fut == nil).To(BeFalse())

			var req *intra.AppendEntriesRequest
			Expect(t, t.mockNodeRaftClient.AppendEntriesInput.In).To(ViaPolling(
				Chain(Receive(), Fetch(&req)),
			))
			Expect(t, req.Leader).To(Equal([]byte("some-leader")))
			//				futs <- fut
			//			}()

			t.mockNodeRaftClient.AppendEntriesOutput.Ret0 <- &intra.AppendEntriesResponse{
				Term: 99,
			}

			var rxFutA, rxFutB rafthashi.AppendFuture
			Expect(t, t.pipeline.Consumer()).To(ViaPolling(
				Chain(Receive(), Fetch(&rxFutA)),
			))
			rxFutB = fut
			// Expect(t, futs).To(ViaPolling(
			// 	Chain(Receive(), Fetch(&rxFutB)),
			// ))

			Expect(t, rxFutA).To(Equal(rxFutB))
			Expect(t, rxFutA.Request()).To(Equal(args))
			Expect(t, rxFutA.Response().Term).To(Equal(uint64(99)))
		})
	})
}
