package raft_test

import (
	"fmt"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/apoydence/eachers/testhelpers"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/api/intra"
	"github.com/apoydence/talaria/internal/end2end"
	"github.com/apoydence/talaria/node/internal/raft"
	rafthashi "github.com/hashicorp/raft"
)

type TT struct {
	*testing.T
	addr               string
	trans              rafthashi.Transport
	mockClientFetcher  *mockClientFetcher
	mockNodeRaftServer []*mockNodeRaftServer
	peers              []string
	closers            []io.Closer
	consumer           chan rafthashi.RPC
}

func TestTransport(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		mockClientFetcher := newMockClientFetcher()

		addr := fmt.Sprintf("localhost:%d", end2end.AvailablePort())
		consumer := make(chan rafthashi.RPC, 100)

		return TT{
			T:                 t,
			addr:              addr,
			mockClientFetcher: mockClientFetcher,
			consumer:          consumer,
			trans:             raft.NewTransport("some-buffer", addr, mockClientFetcher, consumer),
		}
	})

	o.AfterEach(func(t TT) {
		for _, closer := range t.closers {
			closer.Close()
		}
	})

	o.Group("LocalAddr()", func() {
		o.Spec("it returns the local address", func(t TT) {
			Expect(t, t.trans.LocalAddr()).To(Equal(t.addr))
		})
	})

	o.Group("EncodePeer()", func() {
		o.Spec("it encodes the peer", func(t TT) {
			Expect(t, t.trans.EncodePeer("some-peer")).To(Equal([]byte("some-peer")))
		})
	})

	o.Group("DecodePeer()", func() {
		o.Spec("it decodes the peer", func(t TT) {
			Expect(t, t.trans.DecodePeer([]byte("some-peer"))).To(Equal("some-peer"))
		})
	})

	o.Group("AppendEntries()", func() {
		o.BeforeEach(func(t TT) TT {
			server, addr, closer := startMockNodeServer()
			t.peers = append(t.peers, addr)
			t.mockNodeRaftServer = append(t.mockNodeRaftServer, server)
			t.closers = append(t.closers, closer)

			testhelpers.AlwaysReturn(server.AppendEntriesOutput.Ret0, new(intra.AppendEntriesResponse))
			close(server.AppendEntriesOutput.Ret1)

			testhelpers.AlwaysReturn(server.RequestVoteOutput.Ret0, new(intra.RequestVoteResponse))
			close(server.RequestVoteOutput.Ret1)

			client, closer := startNodeClient(addr)
			t.closers = append(t.closers, closer)
			t.mockClientFetcher.FetchOutput.Ret0 <- client

			return t
		})

		o.Spec("it redirects append requests to the correct node", func(t TT) {
			req := rafthashi.AppendEntriesRequest{
				Entries: []*rafthashi.Log{
					{Data: []byte("a")},
					{Data: []byte("b")},
				},
			}

			var resp rafthashi.AppendEntriesResponse
			err := t.trans.AppendEntries(t.peers[0], &req, &resp)
			Expect(t, err == nil).To(BeTrue())

			var rxReq *intra.AppendEntriesRequest
			Expect(t, t.mockNodeRaftServer[0].AppendEntriesInput.Arg1).To(ViaPolling(
				Chain(Receive(), Fetch(&rxReq)),
			))

			Expect(t, rxReq.BufferName).To(Equal("some-buffer"))
			Expect(t, rxReq.Entries).To(HaveLen(2))
			Expect(t, rxReq.Entries[0].Data).To(Equal([]byte("a")))
			Expect(t, rxReq.Entries[1].Data).To(Equal([]byte("b")))
		})

		o.Spec("it redirects append requests via a pipeline to the correct node", func(t TT) {
			req := rafthashi.AppendEntriesRequest{
				Entries: []*rafthashi.Log{
					{Data: []byte("a")},
					{Data: []byte("b")},
				},
			}

			pipeline, err := t.trans.AppendEntriesPipeline(t.peers[0])
			Expect(t, err == nil).To(BeTrue())

			var resp rafthashi.AppendEntriesResponse
			fut, err := pipeline.AppendEntries(&req, &resp)
			Expect(t, err == nil).To(BeTrue())
			Expect(t, fut.Error() == nil).To(BeTrue())

			var rxReq *intra.AppendEntriesRequest
			Expect(t, t.mockNodeRaftServer[0].AppendEntriesInput.Arg1).To(ViaPolling(
				Chain(Receive(), Fetch(&rxReq)),
			))

			Expect(t, rxReq.BufferName).To(Equal("some-buffer"))
			Expect(t, rxReq.Entries).To(HaveLen(2))
			Expect(t, rxReq.Entries[0].Data).To(Equal([]byte("a")))
			Expect(t, rxReq.Entries[1].Data).To(Equal([]byte("b")))
		})

		o.Spec("it redirects vote requests to the correct node", func(t TT) {
			req := rafthashi.RequestVoteRequest{
				Candidate: []byte("some-candidate"),
			}

			var resp rafthashi.RequestVoteResponse
			err := t.trans.RequestVote(t.peers[0], &req, &resp)
			Expect(t, err == nil).To(BeTrue())

			var rxReq *intra.RequestVoteRequest
			Expect(t, t.mockNodeRaftServer[0].RequestVoteInput.Arg1).To(ViaPolling(
				Chain(Receive(), Fetch(&rxReq)),
			))

			Expect(t, rxReq.BufferName).To(Equal("some-buffer"))
			Expect(t, rxReq.Candidate).To(Equal([]byte("some-candidate")))
		})
	})

	o.Group("Consumer()", func() {
		o.Spec("it repeats data from channel to Consumer channel", func(t TT) {
			rpc := rafthashi.RPC{
				Command: "something",
			}

			t.consumer <- rpc

			Expect(t, t.trans.Consumer()).To(ViaPolling(Chain(Receive(), Equal(rpc))))
		})
	})
}

func startMockNodeServer() (*mockNodeRaftServer, string, io.Closer) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	mockNodeRaftServer := newMockNodeRaftServer()
	intra.RegisterNodeRaftServer(s, mockNodeRaftServer)

	go s.Serve(lis)

	return mockNodeRaftServer, lis.Addr().String(), lis
}

func startNodeClient(addr string) (intra.NodeRaftClient, io.Closer) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	c := intra.NewNodeRaftClient(conn)
	return c, conn
}
