//go:generate hel
package server_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
	"github.com/poy/talaria/api/intra"
	pb "github.com/poy/talaria/api/v1"
	"github.com/poy/talaria/scheduler/internal/server"
	"golang.org/x/net/context"
)

func TestMain(m *testing.M) {
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}

	os.Exit(m.Run())
}

type TT struct {
	*testing.T
	mockNodeFetcher *mockNodeFetcher
	mockNodeClient  *mockNodeClient
	mockAuditor     *mockAuditor
	createInfo      *pb.CreateInfo
	nodeURI         string
	s               *server.Server
}

func TestCreateServerNodesAvailable(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		mockNodeClient := newMockNodeClient()
		mockNodeFetcher := newMockNodeFetcher()
		mockAuditor := newMockAuditor()
		nodeURI := "some-uri"

		s := server.New(mockNodeFetcher, mockAuditor)

		createInfo := &pb.CreateInfo{
			Name:       "some-name",
			BufferSize: 99,
		}

		info := server.NodeInfo{
			Client: mockNodeClient,
			URI:    "some-leader-uri",
		}

		mockNodeFetcher.FetchNodesOutput.Client <- []server.NodeInfo{info, info, info}
		mockNodeFetcher.FetchNodesOutput.Client <- []server.NodeInfo{info, info, info}

		return TT{
			T:               t,
			mockNodeFetcher: mockNodeFetcher,
			mockNodeClient:  mockNodeClient,
			mockAuditor:     mockAuditor,
			createInfo:      createInfo,
			nodeURI:         nodeURI,
			s:               s,
		}
	})

	o.Group("when node doesnt return an error", func() {
		o.BeforeEach(func(t TT) TT {
			close(t.mockNodeFetcher.FetchNodesOutput.Client)

			close(t.mockNodeClient.CreateOutput.Ret0)
			close(t.mockNodeClient.CreateOutput.Ret1)
			return t
		})

		o.Spec("it does not return an error", func(t TT) {
			_, err := t.s.Create(context.Background(), t.createInfo)
			Expect(t, err == nil).To(BeTrue())
		})

		o.Spec("it fetches a new node each time", func(t TT) {
			t.s.Create(context.Background(), t.createInfo)
			t.s.Create(context.Background(), t.createInfo)

			Expect(t, t.mockNodeFetcher.FetchNodesCalled).To(ViaPolling(
				HaveLen(2),
			))
		})

		o.Spec("it sets default buffer size", func(t TT) {
			t.createInfo.BufferSize = 0
			t.s.Create(context.Background(), t.createInfo)

			var info *intra.CreateInfo
			Expect(t, t.mockNodeClient.CreateInput.In).To(ViaPolling(
				Chain(Receive(), Fetch(&info)),
			))

			Expect(t, info.BufferSize).To(Equal(uint64(100)))
		})

		o.Spec("it writes to fetched node", func(t TT) {
			t.s.Create(context.Background(), t.createInfo)

			var info *intra.CreateInfo
			Expect(t, t.mockNodeClient.CreateInput.In).To(ViaPolling(
				Chain(Receive(), Fetch(&info)),
			))

			Expect(t, info.Name).To(Equal(t.createInfo.Name))
			Expect(t, info.BufferSize).To(Equal(uint64(99)))
			Expect(t, info.Peers).To(Equal([]*intra.PeerInfo{{"some-leader-uri"}, {"some-leader-uri"}, {"some-leader-uri"}}))
		})
	})

	o.Group("when node returns an error", func() {
		o.BeforeEach(func(t TT) TT {
			t.mockNodeClient.CreateOutput.Ret1 <- fmt.Errorf("some-error")

			close(t.mockNodeFetcher.FetchNodesOutput.Client)

			close(t.mockNodeClient.CreateOutput.Ret0)
			close(t.mockNodeClient.CreateOutput.Ret1)

			return t
		})

		o.Spec("it skips it", func(t TT) {
			_, err := t.s.Create(context.Background(), t.createInfo)
			Expect(t, err == nil).To(BeTrue())
		})
	})

	o.Group("when all the nodes return an error", func() {
		o.BeforeEach(func(t TT) TT {
			info := server.NodeInfo{
				Client: t.mockNodeClient,
				URI:    "some-leader-uri",
			}

			for i := 0; i < 10; i++ {
				t.mockNodeClient.CreateOutput.Ret1 <- fmt.Errorf("some-error")
				t.mockNodeFetcher.FetchNodesOutput.Client <- []server.NodeInfo{info}
			}

			close(t.mockNodeFetcher.FetchNodesOutput.Client)

			close(t.mockNodeClient.CreateOutput.Ret0)
			close(t.mockNodeClient.CreateOutput.Ret1)
			return t
		})

		o.Spec("it gives returns an error", func(t TT) {
			_, err := t.s.Create(context.Background(), t.createInfo)
			Expect(t, err == nil).To(BeFalse())
		})
	})
}

func TestCreateServerNodesNotAvailable(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		mockNodeFetcher := newMockNodeFetcher()
		mockAuditor := newMockAuditor()
		nodeURI := "some-uri"

		s := server.New(mockNodeFetcher, mockAuditor)

		createInfo := &pb.CreateInfo{
			Name: "some-name",
		}

		close(mockNodeFetcher.FetchNodesOutput.Client)

		return TT{
			T:               t,
			mockNodeFetcher: mockNodeFetcher,
			mockAuditor:     mockAuditor,
			createInfo:      createInfo,
			nodeURI:         nodeURI,
			s:               s,
		}
	})

	o.Spec("it returns an error", func(t TT) {
		_, err := t.s.Create(context.Background(), t.createInfo)
		Expect(t, err == nil).To(BeFalse())
	})
}

type TL struct {
	TT
	listResponse pb.ListResponse
}

func TestListClusterInfo(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TL {
		mockNodeClient := newMockNodeClient()
		mockNodeFetcher := newMockNodeFetcher()
		mockAuditor := newMockAuditor()

		s := server.New(mockNodeFetcher, mockAuditor)

		listResp := pb.ListResponse{
			Info: []*pb.ClusterInfo{
				{
					Name:  "A",
					Nodes: []*pb.NodeInfo{{"i"}, {"j"}},
				},
				{
					Name:  "B",
					Nodes: []*pb.NodeInfo{{"k"}, {"l"}},
				},
				{
					Name:  "C",
					Nodes: []*pb.NodeInfo{{"m"}, {"n"}},
				},
			},
		}
		mockAuditor.ListOutput.Ret0 <- listResp

		tt := TT{
			T:               t,
			mockNodeFetcher: mockNodeFetcher,
			mockNodeClient:  mockNodeClient,
			mockAuditor:     mockAuditor,
			s:               s,
		}

		return TL{
			TT:           tt,
			listResponse: listResp,
		}
	})

	o.Spec("it returns all the results from the auditor", func(t TL) {
		resp, err := t.s.ListClusterInfo(context.Background(), new(pb.ListInfo))
		Expect(t, err == nil).To(BeTrue())
		Expect(t, resp).To(Equal(&t.listResponse))
	})

	o.Spec("it returns the requested results from the auditor", func(t TL) {
		resp, err := t.s.ListClusterInfo(context.Background(), &pb.ListInfo{
			Names: []string{"B", "C"},
		})
		Expect(t, err == nil).To(BeTrue())
		Expect(t, resp.Info).To(Equal(t.listResponse.Info[1:]))
	})
}
