package nodefetcher_test

import (
	"io/ioutil"
	"log"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/apoydence/talaria/scheduler/internal/nodefetcher"
	"github.com/apoydence/talaria/scheduler/internal/server"
)

func TestMain(m *testing.M) {
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}

	os.Exit(m.Run())
}

type TT struct {
	*testing.T
	nodeFetcher *nodefetcher.NodeFetcher
	serverURIs  []string
	mockNodes   []*mockNode
	createInfo  *intra.CreateInfo
}

func TestFetchNode(t *testing.T) {
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
			URI, m := startGRPCServer(99)
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

	o.Spec("it returns the matching address", func(t TT) {
		node, err := t.nodeFetcher.FetchNode(t.serverURIs[1])
		Expect(t, err == nil).To(BeTrue())
		node.Client.ReadOnly(context.Background(), new(intra.ReadOnlyInfo))
		Expect(t, t.mockNodes[1].r).To(ViaPolling(HaveLen(1)))
	})
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
			URI, m := startGRPCServer(99)
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
		nodes := t.nodeFetcher.FetchNodes(3)
		Expect(t, nodes).To(HaveLen(3))
	})

	o.Spec("selects random nodes each time", func(t TT) {
		for i := 0; i < 100; i++ {
			nodes := t.nodeFetcher.FetchNodes(3)
			Expect(t, nodes).To(HaveLen(3))

			for _, node := range nodes {
				node.Client.Create(context.Background(), t.createInfo)
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

	o.Spec("excludes given nodes", func(t TT) {
		exclude := t.nodeFetcher.FetchNodes(3)

		nodes := t.nodeFetcher.FetchNodes(3, exclude...)
		Expect(t, nodes).To(HaveLen(3))
		Expect(t, nodes).To(Not(Contain(these(exclude)...)))
	})

	o.Spec("returns what it can when most the nodes are excluded", func(t TT) {
		exclude := t.nodeFetcher.FetchNodes(9)
		Expect(t, exclude).To(HaveLen(9))

		nodes := t.nodeFetcher.FetchNodes(3, exclude...)
		Expect(t, nodes).To(HaveLen(1))
		Expect(t, nodes).To(Not(Contain(these(exclude)...)))
	})

}

func TestNodeFetcherAllNodes(t *testing.T) {
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
			URI, m := startGRPCServer(uint64(i))
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

	o.Spec("it returns all the nodes", func(t TT) {
		nodes := t.nodeFetcher.FetchAllNodes()
		Expect(t, nodes).To(HaveLen(10))
	})

}

func these(ns []server.NodeInfo) []interface{} {
	var result []interface{}
	for _, n := range ns {
		result = append(result, n)
	}
	return result
}

func TestNodeFetcherNodesAreNotConfigured(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.Spec("returns nil", func(t *testing.T) {
		nodeFetcher := nodefetcher.New(nil)
		nodes := nodeFetcher.FetchNodes(3)
		Expect(t, nodes).To(HaveLen(0))
	})
}

type mockNode struct {
	c  chan bool
	r  chan bool
	s  chan bool
	id uint64
}

func (m *mockNode) Create(ctx context.Context, info *intra.CreateInfo) (*intra.CreateResponse, error) {
	m.c <- true
	return new(intra.CreateResponse), nil
}

func (m *mockNode) ReadOnly(ctx context.Context, info *intra.ReadOnlyInfo) (*intra.ReadOnlyResponse, error) {
	m.r <- true
	return new(intra.ReadOnlyResponse), nil
}

func (m *mockNode) Leader(ctx context.Context, req *intra.LeaderRequest) (*intra.LeaderResponse, error) {
	return nil, nil
}

func (m *mockNode) Status(ctx context.Context, req *intra.StatusRequest) (*intra.StatusResponse, error) {
	m.s <- true

	return &intra.StatusResponse{}, nil
}

func (m *mockNode) UpdateConfig(ctx context.Context, req *intra.UpdateConfigRequest) (*intra.UpdateConfigResponse, error) {
	return nil, nil
}

func startGRPCServer(mockID uint64) (string, *mockNode) {
	m := &mockNode{
		c:  make(chan bool, 100),
		r:  make(chan bool, 100),
		s:  make(chan bool, 100),
		id: mockID,
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
