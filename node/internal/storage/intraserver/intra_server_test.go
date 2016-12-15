//go:generate hel
package intraserver_test

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/node/internal/storage/intraserver"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/coreos/etcd/raft/raftpb"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		grpclog.SetLogger(log.New(ioutil.Discard, "", log.LstdFlags))
	}

	os.Exit(m.Run())
}

type TC struct {
	*testing.T
	client        intra.NodeClient
	s             *intraserver.IntraServer
	mockIOFetcher *mockIOFetcher
	mockRouter    *mockRouter
	lis           net.Listener
	conn          *grpc.ClientConn
}

func TestIntraCreate(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TC {
		mockIOFetcher := newMockIOFetcher()
		mockRouter := newMockRouter()

		s := intraserver.New(103, mockIOFetcher, mockRouter)
		URI, lis := setupGrpcServer(s)
		client, conn := establishClient(URI)

		return TC{
			T:             t,
			client:        client,
			s:             s,
			mockIOFetcher: mockIOFetcher,
			mockRouter:    mockRouter,
			lis:           lis,
			conn:          conn,
		}
	})

	o.AfterEach(func(t TC) {
		t.lis.Close()
		t.conn.Close()
	})

	o.Group("when fetcher does not return an error", func() {
		o.BeforeEach(func(t TC) TC {
			t.mockIOFetcher.CreateOutput.Ret0 <- nil
			return t
		})

		o.Spec("it does not return an error", func(t TC) {
			peers := []*intra.PeerInfo{
				{
					Id: 99,
				},
				{
					Id: 101,
				},
			}

			_, err := t.client.Create(context.Background(), &intra.CreateInfo{
				Name:  "some-buffer",
				Peers: peers,
			})
			Expect(t, err == nil).To(Equal(true))
			Expect(t, t.mockIOFetcher.CreateInput.Name).To(ViaPolling(
				Chain(Receive(), Equal("some-buffer")),
			))

			Expect(t, t.mockIOFetcher.CreateInput.Peers).To(ViaPolling(
				Chain(Receive(), Equal(peers)),
			))
		})
	})

	o.Group("when fetcher returns an error on Create", func() {
		o.BeforeEach(func(t TC) TC {
			t.mockIOFetcher.CreateOutput.Ret0 <- fmt.Errorf("some-error")
			return t
		})

		o.Spec("it returns an error", func(t TC) {
			_, err := t.client.Create(context.Background(), &intra.CreateInfo{
				Name: "some-buffer",
			})
			Expect(t, err).To(HaveOccurred())
		})
	})
}

func TestIntraLeader(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TC {
		mockIOFetcher := newMockIOFetcher()
		mockRouter := newMockRouter()

		s := intraserver.New(103, mockIOFetcher, mockRouter)
		URI, lis := setupGrpcServer(s)
		client, conn := establishClient(URI)

		return TC{
			T:             t,
			client:        client,
			s:             s,
			mockIOFetcher: mockIOFetcher,
			mockRouter:    mockRouter,
			lis:           lis,
			conn:          conn,
		}
	})

	o.AfterEach(func(t TC) {
		t.lis.Close()
		t.conn.Close()
	})

	o.Group("when IOFetcher does not return an error", func() {
		o.BeforeEach(func(t TC) TC {
			t.mockIOFetcher.LeaderOutput.Id <- 99
			close(t.mockIOFetcher.LeaderOutput.Err)
			return t
		})

		o.Spec("it returns the leader from the IOFetcher", func(t TC) {
			leader, err := t.client.Leader(context.Background(), &intra.LeaderRequest{
				Name: "some-name",
			})
			Expect(t, err == nil).To(BeTrue())
			Expect(t, leader.Id).To(Equal(uint64(99)))
			Expect(t, t.mockIOFetcher.LeaderInput.Name).To(
				Chain(Receive(), Equal("some-name")))
		})
	})

	o.Group("when IOFetcher returns an error", func() {
		o.BeforeEach(func(t TC) TC {
			close(t.mockIOFetcher.LeaderOutput.Id)
			t.mockIOFetcher.LeaderOutput.Err <- fmt.Errorf("some-error")
			return t
		})

		o.Spec("it returns the leader from the IOFetcher", func(t TC) {
			_, err := t.client.Leader(context.Background(), &intra.LeaderRequest{
				Name: "some-name",
			})
			Expect(t, err == nil).To(BeFalse())
		})
	})
}

func TestIntraStatus(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TC {
		mockIOFetcher := newMockIOFetcher()
		mockRouter := newMockRouter()

		s := intraserver.New(103, mockIOFetcher, mockRouter)
		URI, lis := setupGrpcServer(s)
		client, conn := establishClient(URI)

		return TC{
			T:             t,
			client:        client,
			s:             s,
			mockIOFetcher: mockIOFetcher,
			mockRouter:    mockRouter,
			lis:           lis,
			conn:          conn,
		}
	})

	o.AfterEach(func(t TC) {
		t.lis.Close()
		t.conn.Close()
	})

	o.Spec("it returns the given ID", func(t TC) {
		values := []*intra.StatusBufferInfo{
			{Name: "A", Ids: []uint64{1, 2}},
			{Name: "B", Ids: []uint64{3, 4}},
		}
		t.mockIOFetcher.ListOutput.Ret0 <- values

		status, err := t.client.Status(context.Background(), new(intra.StatusRequest))
		Expect(t, err == nil).To(BeTrue())
		Expect(t, status.Id).To(Equal(uint64(103)))
		Expect(t, status.Buffers).To(Equal(values))
	})
}

type TR struct {
	TC
	msg *intra.UpdateMessage
}

func TestIntraUpdate(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TR {
		mockIOFetcher := newMockIOFetcher()
		mockRouter := newMockRouter()

		s := intraserver.New(103, mockIOFetcher, mockRouter)
		URI, lis := setupGrpcServer(s)
		client, conn := establishClient(URI)

		tc := TC{
			T:             t,
			client:        client,
			s:             s,
			mockIOFetcher: mockIOFetcher,
			mockRouter:    mockRouter,
			lis:           lis,
			conn:          conn,
		}

		msg := &intra.UpdateMessage{
			Name: "some-name",
			Messages: []*raftpb.Message{{
				To: 103,
			}},
		}

		return TR{
			TC:  tc,
			msg: msg,
		}
	})

	o.AfterEach(func(t TR) {
		t.lis.Close()
		t.conn.Close()
	})

	o.Group("when messages have correct ID", func() {
		o.Spec("it sends the messages to the router", func(t TR) {
			_, err := t.client.Update(context.Background(), t.msg)
			Expect(t, err == nil).To(BeTrue())

			Expect(t, t.mockRouter.RouteInput.BufferName).To(ViaPolling(
				Chain(Receive(), Equal("some-name")),
			))

			expectedMsgs := []raftpb.Message{{
				To: 103,
			}}
			Expect(t, t.mockRouter.RouteInput.Msgs).To(ViaPolling(
				Chain(Receive(), Equal(expectedMsgs)),
			))
		})
	})

	o.Group("when messages have an invalid ID", func() {
		o.BeforeEach(func(t TR) TR {
			t.msg.Messages[0].To = 99
			return t
		})

		o.Spec("it returns an InvalidID response", func(t TR) {
			_, err := t.client.Update(context.Background(), t.msg)
			Expect(t, err == nil).To(BeFalse())
			Expect(t, t.mockRouter.RouteCalled).To(Always(HaveLen(0)))
		})
	})
}

func TestIntraUpdateConfig(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TR {
		mockIOFetcher := newMockIOFetcher()
		mockRouter := newMockRouter()

		s := intraserver.New(103, mockIOFetcher, mockRouter)
		URI, lis := setupGrpcServer(s)
		client, conn := establishClient(URI)

		tc := TC{
			T:             t,
			client:        client,
			s:             s,
			mockIOFetcher: mockIOFetcher,
			mockRouter:    mockRouter,
			lis:           lis,
			conn:          conn,
		}

		msg := &intra.UpdateMessage{
			Name: "some-name",
			Messages: []*raftpb.Message{{
				To: 103,
			}},
		}

		return TR{
			TC:  tc,
			msg: msg,
		}
	})

	o.AfterEach(func(t TR) {
		t.lis.Close()
		t.conn.Close()
	})

	o.Group("when the IOFetcher does not return an error", func() {
		o.BeforeEach(func(t TR) TR {
			close(t.mockIOFetcher.UpdateConfigOutput.Ret0)
			return t
		})

		o.Spec("it updates the config", func(t TR) {
			change := raftpb.ConfChange{ID: 99}
			req := intra.UpdateConfigRequest{
				Name:   "some-name",
				Change: &change,
			}
			_, err := t.client.UpdateConfig(context.Background(), &req)
			Expect(t, err == nil).To(BeTrue())

			Expect(t, t.mockIOFetcher.UpdateConfigInput.Name).To(ViaPolling(
				Chain(Receive(), Equal(req.Name)),
			))

			Expect(t, t.mockIOFetcher.UpdateConfigInput.Change).To(ViaPolling(
				Chain(Receive(), Equal(change)),
			))
		})
	})

	o.Group("when the IOFetcher returns an error", func() {
		o.BeforeEach(func(t TR) TR {
			t.mockIOFetcher.UpdateConfigOutput.Ret0 <- fmt.Errorf("some-error")
			return t
		})

		o.Spec("it returns an error", func(t TR) {
			_, err := t.client.UpdateConfig(context.Background(), &intra.UpdateConfigRequest{
				Name: "some-name",
				Change: &raftpb.ConfChange{
					ID: 99,
				},
			})
			Expect(t, err == nil).To(BeFalse())
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
