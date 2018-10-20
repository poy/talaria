package network_test

import (
	"context"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/poy/eachers/testhelpers"
	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
	"github.com/poy/talaria/api/intra"
	"github.com/poy/talaria/node/internal/raft/network"
)

type TCC struct {
	*testing.T
	clientCache *network.ClientCache
	closers     []io.Closer
	servers     []*mockNodeRaftServer
	addrs       []string
}

func TestClientCache(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TCC {
		tcc := TCC{
			T:           t,
			clientCache: network.NewClientCache(),
		}

		for i := 0; i < 3; i++ {
			server, addr, closer := startMockNodeServer()
			tcc.servers = append(tcc.servers, server)
			tcc.addrs = append(tcc.addrs, addr)
			tcc.closers = append(tcc.closers, closer)

			testhelpers.AlwaysReturn(server.AppendEntriesOutput.Ret0, new(intra.AppendEntriesResponse))
			close(server.AppendEntriesOutput.Ret1)
		}

		return tcc
	})

	o.AfterEach(func(t TCC) {
		for _, closer := range t.closers {
			closer.Close()
		}
	})

	o.Spec("it establishes a connection with the right server", func(t TCC) {
		client := t.clientCache.Fetch(t.addrs[0])
		_, err := client.AppendEntries(context.Background(), &intra.AppendEntriesRequest{})
		Expect(t, err == nil).To(BeTrue())
		Expect(t, t.servers[0].AppendEntriesCalled).To(ViaPolling(HaveLen(1)))
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
