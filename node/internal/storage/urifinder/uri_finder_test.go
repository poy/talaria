//go:generate hel

package urifinder_test

import (
	"fmt"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/node/internal/storage/urifinder"
	"github.com/apoydence/talaria/pb/intra"
)

type TT struct {
	*testing.T
	mockSchedulerServer *mockSchedulerServer
	closer              io.Closer
	finder              *urifinder.URIFinder
}

func TestFromID(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		return TT{
			T: t,
		}
	})

	o.Group("scheduler available", func() {
		o.BeforeEach(func(t TT) TT {
			addr, mockSchedulerServer, closer := startGRPCServer()
			t.closer = closer
			t.mockSchedulerServer = mockSchedulerServer
			t.finder = urifinder.New(addr)

			return t
		})

		o.Group("when scheduler does not return an error", func() {
			o.BeforeEach(func(t TT) TT {
				t.mockSchedulerServer.FromIDOutput.Ret0 <- &intra.FromIdResponse{
					Uri: "some-uri",
				}
				close(t.mockSchedulerServer.FromIDOutput.Ret1)
				return t
			})

			o.Spec("it returns the result from the scheduler", func(t TT) {
				uri, err := t.finder.FromID(99)
				Expect(t, err == nil).To(BeTrue())
				Expect(t, uri).To(Equal("some-uri"))
			})
		})

		o.Group("scheduler returns an error", func() {
			o.BeforeEach(func(t TT) TT {
				close(t.mockSchedulerServer.FromIDOutput.Ret0)
				t.mockSchedulerServer.FromIDOutput.Ret1 <- fmt.Errorf("some-error")
				return t
			})

			o.Spec("it returns an error", func(t TT) {
				_, err := t.finder.FromID(99)
				Expect(t, err == nil).To(BeFalse())
			})
		})
	})

	o.Group("scheduler not available", func() {
		o.BeforeEach(func(t TT) TT {
			t.finder = urifinder.New("invalid")
			return t
		})

		o.Spec("it returns an error", func(t TT) {
			_, err := t.finder.FromID(99)
			Expect(t, err == nil).To(BeFalse())
		})
	})
}

func startGRPCServer() (string, *mockSchedulerServer, io.Closer) {
	mockSchedulerServer := newMockSchedulerServer()
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	intra.RegisterSchedulerServer(s, mockSchedulerServer)
	reflection.Register(s)
	go s.Serve(lis)

	return lis.Addr().String(), mockSchedulerServer, lis
}
