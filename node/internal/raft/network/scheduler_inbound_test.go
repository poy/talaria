//go:generate hel

package network_test

import (
	"context"
	"io"
	"testing"

	"google.golang.org/grpc"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/api/intra"
	"github.com/apoydence/talaria/node/internal/raft/network"
)

type TIS struct {
	*testing.T
	schedulerHandler *network.SchedulerInbound
	mockIOFetcher    *mockIOFetcher
}

func TestInboundSchedulerCreate(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	setupTIS(o)

	o.Spec("it creates the buffer", func(t TIS) {
		close(t.mockIOFetcher.CreateOutput.Ret0)

		_, err := t.schedulerHandler.Create(context.Background(), &intra.CreateInfo{
			Name:       "some-buffer",
			BufferSize: 99,
			Peers: []*intra.PeerInfo{
				{"A"}, {"B"}, {"C"},
			},
		})

		Expect(t, err == nil).To(BeTrue())
		Expect(t, t.mockIOFetcher.CreateInput.Name).To(ViaPolling(
			Chain(Receive(), Equal("some-buffer")),
		))
		Expect(t, t.mockIOFetcher.CreateInput.BufferSize).To(ViaPolling(
			Chain(Receive(), Equal(uint64(99))),
		))
		Expect(t, t.mockIOFetcher.CreateInput.Peers).To(ViaPolling(
			Chain(Receive(), Equal([]string{"A", "B", "C"})),
		))
	})

	o.Spec("it returns an error for a 0 buffer size", func(t TIS) {
		close(t.mockIOFetcher.CreateOutput.Ret0)

		_, err := t.schedulerHandler.Create(context.Background(), &intra.CreateInfo{
			Name:       "some-buffer",
			BufferSize: 0,
			Peers: []*intra.PeerInfo{
				{"A"}, {"B"}, {"C"},
			},
		})

		Expect(t, err == nil).To(BeFalse())
	})
}

func TestInboundSchedulerLeader(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	setupTIS(o)

	o.Spec("it returns the leader", func(t TIS) {
		t.mockIOFetcher.LeaderOutput.Ret0 <- "some-leader"
		close(t.mockIOFetcher.LeaderOutput.Ret1)

		resp, err := t.schedulerHandler.Leader(context.Background(), &intra.LeaderRequest{
			Name: "some-buffer",
		})

		Expect(t, err == nil).To(BeTrue())
		Expect(t, resp.Addr).To(Equal("some-leader"))
	})
}

func TestInboundSchedulerStatus(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	setupTIS(o)

	o.Spec("it returns the status", func(t TIS) {
		t.mockIOFetcher.StatusOutput.Ret0 <- map[string][]string{
			"A": []string{"A", "B", "C"},
			"B": []string{"A", "B", "C"},
		}

		resp, err := t.schedulerHandler.Status(context.Background(), &intra.StatusRequest{})
		Expect(t, err == nil).To(BeTrue())

		Expect(t, resp.ExternalAddr).To(Equal("some-external-addr"))
		Expect(t, resp.Buffers).To(HaveLen(2))
		Expect(t, resp.Buffers[0].Name).To(Or(Equal("A"), Equal("B")))
		Expect(t, resp.Buffers[1].Name).To(Or(Equal("A"), Equal("B")))
		Expect(t, resp.Buffers[0].Name).To(Not(Equal(resp.Buffers[1].Name)))
		Expect(t, resp.Buffers[0].ExpectedNodes).To(And(
			Contain("A"),
			Contain("B"),
			Contain("C"),
		))
		Expect(t, resp.Buffers[1].ExpectedNodes).To(And(
			Contain("A"),
			Contain("B"),
			Contain("C"),
		))
	})
}

func TestInboundSchedulerUpdateConfig(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	setupTIS(o)

	o.Spec("it returns the status", func(t TIS) {
		close(t.mockIOFetcher.SetExpectedPeersOutput.Ret0)

		_, err := t.schedulerHandler.UpdateConfig(context.Background(), &intra.UpdateConfigRequest{
			Name:          "some-buffer",
			ExpectedNodes: []string{"A", "B", "C"},
		})

		Expect(t, err == nil).To(BeTrue())
		Expect(t, t.mockIOFetcher.SetExpectedPeersInput.ExpectedPeers).To(
			Chain(Receive(), Equal([]string{"A", "B", "C"})),
		)
	})
}

func setupTIS(o *onpar.Onpar) {
	o.BeforeEach(func(t *testing.T) TIS {
		mockIOFetcher := newMockIOFetcher()
		return TIS{
			T:                t,
			schedulerHandler: network.NewSchedulerInbound("some-external-addr", mockIOFetcher),
			mockIOFetcher:    mockIOFetcher,
		}
	})
}

func startNodeClient(addr string) (intra.NodeClient, io.Closer) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	c := intra.NewNodeClient(conn)
	return c, conn
}
