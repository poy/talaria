//go:generate hel

package network_test

import (
	"context"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"google.golang.org/grpc"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/api/intra"
	"github.com/apoydence/talaria/node/internal/raft/network"
	rafthashi "github.com/hashicorp/raft"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}

	os.Exit(m.Run())
}

type TI struct {
	*testing.T
	inbound *network.Inbound
	closers []io.Closer
	client  intra.NodeRaftClient
}

func TestInbound(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TI {
		i := network.NewInbound("localhost:0", newMockNodeClient())
		client, closer := startNodeRaftClient(i.Addr())

		return TI{
			T:       t,
			client:  client,
			closers: []io.Closer{closer},
			inbound: i,
		}
	})

	o.AfterEach(func(t TI) {
		for _, closer := range t.closers {
			closer.Close()
		}
	})

	o.Group("AppendEntries", func() {
		o.Spec("it routes data to the correct buffers", func(t TI) {
			ca := t.inbound.Fetch("buffer-a")
			cb := t.inbound.Fetch("buffer-b")

			resps := make(chan *intra.AppendEntriesResponse)
			go func() {
				resp, err := t.client.AppendEntries(context.Background(), &intra.AppendEntriesRequest{
					BufferName: "buffer-a",
					Leader:     []byte("leader-a"),
				})
				Expect(t, err == nil).To(BeTrue())
				resps <- resp
			}()

			var req rafthashi.RPC
			Expect(t, ca).To(ViaPolling(
				Chain(Receive(), Fetch(&req)),
			))

			appendReq, ok := req.Command.(*rafthashi.AppendEntriesRequest)
			Expect(t, ok).To(BeTrue())
			Expect(t, appendReq.Leader).To(Equal([]byte("leader-a")))
			Expect(t, cb).To(Always(HaveLen(0)))

			req.Respond(&rafthashi.AppendEntriesResponse{
				LastLog: 99,
			}, nil)

			var resp *intra.AppendEntriesResponse
			Expect(t, resps).To(ViaPolling(
				Chain(Receive(), Fetch(&resp)),
			))
			Expect(t, resp.LastLog).To(Equal(uint64(99)))
		})
	})

	o.Group("RequestVote", func() {
		o.Spec("it routes data to the correct buffers", func(t TI) {
			ca := t.inbound.Fetch("buffer-a")
			cb := t.inbound.Fetch("buffer-b")

			resps := make(chan *intra.RequestVoteResponse)
			go func() {
				resp, err := t.client.RequestVote(context.Background(), &intra.RequestVoteRequest{
					BufferName: "buffer-a",
					Candidate:  []byte("candidate-a"),
				})
				Expect(t, err == nil).To(BeTrue())
				resps <- resp
			}()

			var req rafthashi.RPC
			Expect(t, ca).To(ViaPolling(
				Chain(Receive(), Fetch(&req)),
			))

			voteReq, ok := req.Command.(*rafthashi.RequestVoteRequest)
			Expect(t, ok).To(BeTrue())
			Expect(t, voteReq.Candidate).To(Equal([]byte("candidate-a")))
			Expect(t, cb).To(Always(HaveLen(0)))

			req.Respond(&rafthashi.RequestVoteResponse{
				Term: 99,
			}, nil)

			var resp *intra.RequestVoteResponse
			Expect(t, resps).To(ViaPolling(
				Chain(Receive(), Fetch(&resp)),
			))
			Expect(t, resp.Term).To(Equal(uint64(99)))
		})

	})
}

func startNodeRaftClient(addr string) (intra.NodeRaftClient, io.Closer) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	c := intra.NewNodeRaftClient(conn)
	return c, conn
}
