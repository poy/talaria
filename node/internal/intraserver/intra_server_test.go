//go:generate hel
package intraserver_test

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/apoydence/talaria/node/internal/intraserver"
	"github.com/apoydence/talaria/pb/intra"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Intra", func() {
	var (
		mockIOFetcher *mockIOFetcher

		listeners []net.Listener
		conns     []*grpc.ClientConn
		s         *intraserver.IntraServer
		client    intra.NodeClient
	)

	var establishClient = func(URI string) intra.NodeClient {
		conn, err := grpc.Dial(URI, grpc.WithInsecure())
		Expect(err).ToNot(HaveOccurred())
		conns = append(conns, conn)
		return intra.NewNodeClient(conn)
	}

	var setupGrpcServer = func(handler *intraserver.IntraServer) string {
		lis, err := net.Listen("tcp", ":0")
		Expect(err).ToNot(HaveOccurred())
		listeners = append(listeners, lis)
		gs := grpc.NewServer()
		intra.RegisterNodeServer(gs, handler)
		go gs.Serve(lis)
		return lis.Addr().String()
	}

	BeforeEach(func() {
		listeners = nil
		conns = nil
		mockIOFetcher = newMockIOFetcher()

		s = intraserver.New(mockIOFetcher)
		URI := setupGrpcServer(s)
		client = establishClient(URI)
	})

	JustBeforeEach(func() {
		close(mockIOFetcher.CreateOutput.Ret0)
	})

	Describe("Create()", func() {
		Context("fetcher does not return an error", func() {
			It("does not return an error", func() {
				_, err := client.Create(context.Background(), &intra.CreateInfo{
					Name: "some-buffer",
				})
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("fetcher returns an error on Create", func() {
			BeforeEach(func() {
				mockIOFetcher.CreateOutput.Ret0 <- fmt.Errorf("some-error")
			})

			It("returns an error", func() {
				_, err := client.Create(context.Background(), &intra.CreateInfo{
					Name: "some-buffer",
				})
				Expect(err).To(HaveOccurred())
			})
		})
	})
})
