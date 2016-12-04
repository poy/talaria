package nodefetcher_test

import (
	"net"
	"testing"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/apoydence/talaria/scheduler/internal/nodefetcher"
)

type TT struct {
	*testing.T
	nodeFetcher *nodefetcher.NodeFetcher
	serverURIs  []string
	mockNodes   []*mockNode
	createInfo  *intra.CreateInfo
}

func TestNodeFetcherNodesAreConfigured(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		createInfo := &intra.CreateInfo{
			Name: "some-name",
		}

		var serverURIs []string
		var mockNodes []*mockNode

		for i := 0; i < 10; i++ {
			URI, m := startGRPCServer()
			serverURIs = append(serverURIs, URI)
			mockNodes = append(mockNodes, m)
		}

		return TT{
			T:           t,
			nodeFetcher: nodefetcher.New(serverURIs),
			serverURIs:  serverURIs,
			mockNodes:   mockNodes,
			createInfo:  createInfo,
		}
	})

	o.Spec("selects 3 nodes", func(t TT) {
		nodes := t.nodeFetcher.FetchNodes()
		Expect(t, nodes).To(HaveLen(3))
	})

	o.Spec("selects random nodes each time", func(t TT) {
		for i := 0; i < 100; i++ {
			nodes := t.nodeFetcher.FetchNodes()
			Expect(t, nodes).To(HaveLen(3))

			for _, node := range nodes {
				node.Client.Create(context.Background(), t.createInfo)
				Expect(t, node.ID).To(Equal(uint64(99)))
				Expect(t, node.URI).To(Not(HaveLen(0)))
			}
		}

		Expect(t, float64(len(t.mockNodes[0].c))).To(Chain(
			BeAbove(float64(len(t.mockNodes[1].c))-20),
			BeBelow(float64(len(t.mockNodes[1].c))+20),
		))

		Expect(t, float64(len(t.mockNodes[0].c))).To(Chain(
			BeAbove(float64(len(t.mockNodes[2].c))-20),
			BeBelow(float64(len(t.mockNodes[2].c))+20),
		))
	})

}

func TestNodeFetcherNodesAreNotConfigured(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.Spec("returns nil", func(t *testing.T) {
		nodeFetcher := nodefetcher.New(nil)
		nodes := nodeFetcher.FetchNodes()
		Expect(t, nodes).To(HaveLen(0))
	})
}

type mockNode struct {
	c chan bool
	s chan bool
}

func (m *mockNode) Create(ctx context.Context, info *intra.CreateInfo) (*intra.CreateResponse, error) {
	m.c <- true
	return new(intra.CreateResponse), nil
}

func (m *mockNode) Update(ctx context.Context, in *intra.UpdateMessage) (*intra.UpdateResponse, error) {
	return nil, nil
}

func (m *mockNode) Leader(ctx context.Context, req *intra.LeaderRequest) (*intra.LeaderInfo, error) {
	return nil, nil
}

func (m *mockNode) Status(ctx context.Context, req *intra.StatusRequest) (*intra.StatusResponse, error) {
	m.s <- true

	return &intra.StatusResponse{
		Id: 99,
	}, nil
}

func startGRPCServer() (string, *mockNode) {
	m := &mockNode{
		c: make(chan bool, 100),
		s: make(chan bool, 100),
	}
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()

	intra.RegisterNodeServer(grpcServer, m)
	go grpcServer.Serve(lis)

	return lis.Addr().String(), m
}
