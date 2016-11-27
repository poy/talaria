//go:generate hel
package intraserver_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/node/internal/storage/intraserver"
	"github.com/apoydence/talaria/pb/intra"
)

type TT struct {
	*testing.T
	client        intra.NodeClient
	s             *intraserver.IntraServer
	mockIOFetcher *mockIOFetcher
	lis           net.Listener
	conn          *grpc.ClientConn
}

func TestIntra(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		mockIOFetcher := newMockIOFetcher()

		s := intraserver.New(mockIOFetcher)
		URI, lis := setupGrpcServer(s)
		client, conn := establishClient(URI)

		return TT{
			T:             t,
			client:        client,
			s:             s,
			mockIOFetcher: mockIOFetcher,
			lis:           lis,
			conn:          conn,
		}
	})

	o.AfterEach(func(t TT) {
		t.lis.Close()
		t.conn.Close()
	})

	o.Group("Create()", func() {
		o.Group("when fetcher does not return an error", func() {
			o.BeforeEach(func(t TT) TT {
				t.mockIOFetcher.CreateOutput.Ret0 <- nil
				return t
			})

			o.Spec("it does not return an error", func(t TT) {
				_, err := t.client.Create(context.Background(), &intra.CreateInfo{
					Name: "some-buffer",
				})
				Expect(t, err == nil).To(Equal(true))
			})
		})

		o.Group("when fetcher returns an error on Create", func() {
			o.BeforeEach(func(t TT) TT {
				t.mockIOFetcher.CreateOutput.Ret0 <- fmt.Errorf("some-error")
				return t
			})

			o.Spec("it returns an error", func(t TT) {
				_, err := t.client.Create(context.Background(), &intra.CreateInfo{
					Name: "some-buffer",
				})
				Expect(t, err).To(HaveOccurred())
			})
		})
	})

}

func setupGrpcServer(handler *intraserver.IntraServer) (string, net.Listener) {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	gs := grpc.NewServer()
	intra.RegisterNodeServer(gs, handler)
	go gs.Serve(lis)
	return lis.Addr().String(), lis
}

func establishClient(URI string) (intra.NodeClient, *grpc.ClientConn) {
	conn, err := grpc.Dial(URI, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	return intra.NewNodeClient(conn), conn
}
