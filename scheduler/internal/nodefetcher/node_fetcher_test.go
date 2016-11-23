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

		for i := 0; i < 3; i++ {
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

	o.Spec("selects a random node each time", func(t TT) {
		m := make(map[string]int)
		for i := 0; i < 100; i++ {
			node, URI := t.nodeFetcher.FetchNode()
			Expect(t, node == nil).To(BeFalse())
			node.Create(context.Background(), t.createInfo)
			m[URI]++
		}

		Expect(t, float64(len(t.mockNodes[0].c))).To(Chain(
			BeAbove(float64(len(t.mockNodes[1].c))-20),
			BeBelow(float64(len(t.mockNodes[1].c))+20),
		))

		Expect(t, float64(len(t.mockNodes[0].c))).To(Chain(
			BeAbove(float64(len(t.mockNodes[2].c))-20),
			BeBelow(float64(len(t.mockNodes[2].c))+20),
		))

		Expect(t, m).To(HaveLen(len(t.serverURIs)))
		for _, URI := range t.serverURIs {
			Expect(t, float64(m[URI])).To(Chain(
				BeAbove(float64(100/len(t.serverURIs)-20)),
				BeBelow(float64(100/len(t.serverURIs)+20)),
			))
		}
	})

}

func TestNodeFetcherNodesAreNotConfigured(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.Spec("returns nil", func(t *testing.T) {
		nodeFetcher := nodefetcher.New(nil)
		node, _ := nodeFetcher.FetchNode()
		Expect(t, node == nil).To(BeTrue())
	})
}

type mockNode struct {
	c chan bool
}

func (m *mockNode) Create(ctx context.Context, info *intra.CreateInfo) (*intra.CreateResponse, error) {
	m.c <- true
	return new(intra.CreateResponse), nil
}

func startGRPCServer() (string, *mockNode) {
	m := &mockNode{c: make(chan bool, 100)}
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()

	intra.RegisterNodeServer(grpcServer, m)
	go grpcServer.Serve(lis)

	return lis.Addr().String(), m
}
