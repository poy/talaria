//go:generate hel
package server_test

import (
	"fmt"
	"testing"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/apoydence/talaria/scheduler/internal/server"
	"golang.org/x/net/context"
)

type TT struct {
	*testing.T
	mockNodeFetcher *mockNodeFetcher
	mockNodeClient  *mockNodeClient
	createInfo      *pb.CreateInfo
	nodeURI         string
	s               *server.Server
}

func TestServerNodesAvailable(t *testing.T) {
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

		mockNodeFetcher.FetchNodeOutput.Client <- mockNodeClient
		mockNodeFetcher.FetchNodeOutput.URI <- nodeURI
		mockNodeFetcher.FetchNodeOutput.Client <- mockNodeClient
		mockNodeFetcher.FetchNodeOutput.URI <- nodeURI

		return TT{
			T:               t,
			mockNodeFetcher: mockNodeFetcher,
			mockNodeClient:  mockNodeClient,
			createInfo:      createInfo,
			nodeURI:         nodeURI,
			s:               s,
		}
	})

	o.Group("when node doesn't return an error", func() {
		o.BeforeEach(func(t TT) TT {
			close(t.mockNodeFetcher.FetchNodeOutput.Client)
			close(t.mockNodeFetcher.FetchNodeOutput.URI)

			close(t.mockNodeClient.CreateOutput.Ret0)
			close(t.mockNodeClient.CreateOutput.Ret1)
			return t
		})

		o.Spec("it does not return an error and gives the URI", func(t TT) {
			resp, err := t.s.Create(context.Background(), t.createInfo)
			Expect(t, err == nil).To(BeTrue())
			Expect(t, resp.Uri).To(Equal(t.nodeURI))
		})

		o.Spec("it fetches a new node each time", func(t TT) {
			t.s.Create(context.Background(), t.createInfo)
			t.s.Create(context.Background(), t.createInfo)

			Expect(t, t.mockNodeFetcher.FetchNodeCalled).To(ViaPolling(
				HaveLen(2),
			))
		})

		o.Spec("it writes to fetched node", func(t TT) {
			expected := &intra.CreateInfo{
				Name: t.createInfo.Name,
			}
			t.s.Create(context.Background(), t.createInfo)

			Expect(t, t.mockNodeClient.CreateInput.In).To(ViaPolling(
				Chain(Receive(), Equal(expected)),
			))
		})
	})

	o.Group("when node returns an error", func() {
		o.BeforeEach(func(t TT) TT {
			t.mockNodeClient.CreateOutput.Ret1 <- fmt.Errorf("some-error")

			close(t.mockNodeFetcher.FetchNodeOutput.Client)
			close(t.mockNodeFetcher.FetchNodeOutput.URI)

			close(t.mockNodeClient.CreateOutput.Ret0)
			close(t.mockNodeClient.CreateOutput.Ret1)
			return t
		})

		o.Spec("it tries a new node", func(t TT) {
			_, err := t.s.Create(context.Background(), t.createInfo)
			Expect(t, err == nil).To(BeTrue())

			Expect(t, t.mockNodeFetcher.FetchNodeCalled).To(ViaPolling(
				HaveLen(2),
			))
		})
	})

	o.Group("when nodes always return an error", func() {
		o.BeforeEach(func(t TT) TT {
			for i := 0; i < 10; i++ {
				t.mockNodeClient.CreateOutput.Ret1 <- fmt.Errorf("some-error")
				t.mockNodeFetcher.FetchNodeOutput.Client <- t.mockNodeClient
			}

			close(t.mockNodeFetcher.FetchNodeOutput.Client)
			close(t.mockNodeFetcher.FetchNodeOutput.URI)

			close(t.mockNodeClient.CreateOutput.Ret0)
			close(t.mockNodeClient.CreateOutput.Ret1)
			return t
		})

		o.Spec("it gives up after 5 tries and returns an error", func(t TT) {
			_, err := t.s.Create(context.Background(), t.createInfo)
			Expect(t, err == nil).To(BeFalse())

			Expect(t, t.mockNodeFetcher.FetchNodeCalled).To(ViaPolling(
				HaveLen(5),
			))
		})
	})
}

func TestServerNodesNotAvailable(t *testing.T) {
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

		close(mockNodeFetcher.FetchNodeOutput.Client)
		close(mockNodeFetcher.FetchNodeOutput.URI)

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
