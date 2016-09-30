package nodefetcher_test

import (
	"net"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/apoydence/talaria/pb/intra"
	"github.com/apoydence/talaria/scheduler/internal/nodefetcher"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NodeFetcher", func() {
	var (
		nodeFetcher *nodefetcher.NodeFetcher

		serverURIs []string
		mockNodes  []*mockNode
		createInfo *intra.CreateInfo
	)

	var startGRPCServer = func() (string, *mockNode) {
		m := &mockNode{c: make(chan bool, 100)}
		lis, err := net.Listen("tcp", ":0")
		Expect(err).ToNot(HaveOccurred())

		grpcServer := grpc.NewServer()

		intra.RegisterNodeServer(grpcServer, m)
		go grpcServer.Serve(lis)

		return lis.Addr().String(), m
	}

	BeforeEach(func() {
		serverURIs = nil
		mockNodes = nil

		createInfo = &intra.CreateInfo{
			Name: "some-name",
		}
	})

	JustBeforeEach(func() {
		nodeFetcher = nodefetcher.New(serverURIs)
	})

	Context("nodes are configured", func() {
		BeforeEach(func() {
			for i := 0; i < 3; i++ {
				URI, m := startGRPCServer()
				serverURIs = append(serverURIs, URI)
				mockNodes = append(mockNodes, m)
			}

		})

		Describe("FetchNode()", func() {
			It("selects a random node each time", func() {
				m := make(map[string]int)
				for i := 0; i < 100; i++ {
					node, URI := nodeFetcher.FetchNode()
					Expect(node).ToNot(BeNil())
					node.Create(context.Background(), createInfo)
					m[URI]++
				}

				Eventually(len(mockNodes[0].c)).Should(BeNumerically("~", len(mockNodes[1].c), 20))
				Eventually(len(mockNodes[0].c)).Should(BeNumerically("~", len(mockNodes[2].c), 20))
				Expect(m).To(HaveLen(len(serverURIs)))
				for _, URI := range serverURIs {
					Expect(m[URI]).To(BeNumerically("~", 100/len(serverURIs), 20))
				}
			})
		})
	})

	Context("nodes are not configured", func() {
		It("returns nil", func() {
			Expect(nodeFetcher.FetchNode()).To(BeNil())
		})
	})
})

type mockNode struct {
	c chan bool
}

func (m *mockNode) Create(ctx context.Context, info *intra.CreateInfo) (*intra.CreateResponse, error) {
	m.c <- true
	return new(intra.CreateResponse), nil
}
