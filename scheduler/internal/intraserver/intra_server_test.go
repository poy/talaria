//go:generate hel

package intraserver_test

import (
	"context"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"testing"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/apoydence/talaria/scheduler/internal/intraserver"
	"github.com/apoydence/talaria/scheduler/internal/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func MainTest(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		grpclog.SetLogger(log.New(ioutil.Discard, "", log.LstdFlags))
	}

	os.Exit(m.Run())
}

type TT struct {
	*testing.T
	intraServer     *intraserver.IntraServer
	client          intra.SchedulerClient
	closer          io.Closer
	mockNodeFetcher *mockNodeFetcher
}

func TestFromID(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		mockNodeFetcher := newMockNodeFetcher()

		intraServer := intraserver.New(mockNodeFetcher)
		client, closer := startServer(intraServer)

		return TT{
			T:               t,
			intraServer:     intraServer,
			client:          client,
			closer:          closer,
			mockNodeFetcher: mockNodeFetcher,
		}
	})

	o.Spec("it returns the requested node URI", func(t TT) {
		t.mockNodeFetcher.FetchAllNodesOutput.Ret0 <- []server.NodeInfo{
			{URI: "0", ID: 0},
			{URI: "1", ID: 1},
			{URI: "2", ID: 2},
		}

		resp, err := t.client.FromID(context.Background(), &intra.FromIdRequest{Id: 1})
		Expect(t, err == nil).To(BeTrue())
		Expect(t, resp.Uri).To(Equal("1"))
	})
}

func startServer(server *intraserver.IntraServer) (intra.SchedulerClient, io.Closer) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	intra.RegisterSchedulerServer(s, server)

	go s.Serve(lis)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	c := intra.NewSchedulerClient(conn)

	return c, lis
}
