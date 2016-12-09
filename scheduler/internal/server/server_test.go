//go:generate hel
package server_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/apoydence/eachers/testhelpers"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/apoydence/talaria/scheduler/internal/server"
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
		nodeURI := "some-uri"

		s := server.New(mockNodeFetcher)

		createInfo := &pb.CreateInfo{
			Name: "some-name",
		}

		info := server.NodeInfo{
			Client: mockNodeClient,
			ID:     99,
			URI:    "some-leader-uri",
		}

		mockNodeFetcher.FetchNodesOutput.Client <- []server.NodeInfo{info, info, info}
		mockNodeFetcher.FetchNodesOutput.Client <- []server.NodeInfo{info, info, info}

		return TT{
			T:               t,
			mockNodeFetcher: mockNodeFetcher,
			mockNodeClient:  mockNodeClient,
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

		o.Group("when fetching the Leader does not return an error", func() {
			o.BeforeEach(func(t TT) TT {
				leaderInfo := &intra.LeaderInfo{
					Id: 99,
				}

				for i := 0; i < 3; i++ {
					t.mockNodeClient.StatusOutput.Ret0 <- &intra.StatusResponse{
						Id: 99,
					}
				}

				t.mockNodeClient.LeaderOutput.Ret0 <- leaderInfo
				t.mockNodeClient.LeaderOutput.Ret0 <- leaderInfo
				close(t.mockNodeClient.LeaderOutput.Ret1)
				return t
			})

			o.Spec("it does not return an error and gives the URI", func(t TT) {
				resp, err := t.s.Create(context.Background(), t.createInfo)
				Expect(t, err == nil).To(BeTrue())
				Expect(t, resp.Uri).To(Equal("some-leader-uri"))
			})

			o.Spec("it fetches a new node each time", func(t TT) {
				t.s.Create(context.Background(), t.createInfo)
				t.s.Create(context.Background(), t.createInfo)

				Expect(t, t.mockNodeFetcher.FetchNodesCalled).To(ViaPolling(
					HaveLen(2),
				))
			})

			o.Spec("it writes to fetched node", func(t TT) {
				t.s.Create(context.Background(), t.createInfo)

				var info *intra.CreateInfo
				Expect(t, t.mockNodeClient.CreateInput.In).To(ViaPolling(
					Chain(Receive(), Fetch(&info)),
				))

				Expect(t, info.Name).To(Equal(t.createInfo.Name))
				Expect(t, info.Peers).To(Equal([]*intra.PeerInfo{{Id: 99}, {Id: 99}, {Id: 99}}))
			})
		})

		o.Group("when fetching the leader returns an error", func() {
			o.BeforeEach(func(t TT) TT {
				t.mockNodeClient.LeaderOutput.Ret0 <- nil
				t.mockNodeClient.LeaderOutput.Ret1 <- fmt.Errorf("some-error")

				t.mockNodeClient.LeaderOutput.Ret0 <- &intra.LeaderInfo{
					Id: 99,
				}
				t.mockNodeClient.LeaderOutput.Ret1 <- nil
				return t
			})

			o.Spec("it should try again with each node", func(t TT) {
				resp, err := t.s.Create(context.Background(), t.createInfo)
				Expect(t, err == nil).To(BeTrue())
				Expect(t, resp.Uri).To(Equal("some-leader-uri"))
			})
		})

		o.Group("when fetching the leader always returns an error", func() {
			o.BeforeEach(func(t TT) TT {
				close(t.mockNodeClient.LeaderOutput.Ret0)
				testhelpers.AlwaysReturn(t.mockNodeClient.LeaderOutput.Ret1, fmt.Errorf("some-error"))
				return t
			})

			o.Spec("it should return an error", func(t TT) {
				_, err := t.s.Create(context.Background(), t.createInfo)
				Expect(t, err == nil).To(BeFalse())
			})
		})
	})

	o.Group("when node returns an error", func() {
		o.BeforeEach(func(t TT) TT {
			t.mockNodeClient.CreateOutput.Ret1 <- fmt.Errorf("some-error")

			close(t.mockNodeFetcher.FetchNodesOutput.Client)

			close(t.mockNodeClient.CreateOutput.Ret0)
			close(t.mockNodeClient.CreateOutput.Ret1)

			leaderInfo := &intra.LeaderInfo{
				Id: 99,
			}

			t.mockNodeClient.LeaderOutput.Ret0 <- leaderInfo
			close(t.mockNodeClient.LeaderOutput.Ret1)

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
				ID:     99,
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
		nodeURI := "some-uri"

		s := server.New(mockNodeFetcher)

		createInfo := &pb.CreateInfo{
			Name: "some-name",
		}

		close(mockNodeFetcher.FetchNodesOutput.Client)

		return TT{
			T:               t,
			mockNodeFetcher: mockNodeFetcher,
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
