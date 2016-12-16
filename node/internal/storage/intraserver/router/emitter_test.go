//go:generate hel

package router_test

import (
	"fmt"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/node/internal/storage/intraserver/router"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/coreos/etcd/raft/raftpb"
)

type TE struct {
	*testing.T
	mockURIFinder   *mockURIFinder
	msgs            []raftpb.Message
	mockNodeServers []*mockNodeServer
	nodeAddrs       []string
	closers         []io.Closer
	emitter         *router.Emitter
}

func TestEmitterEmit(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TE {

		mockURIFinder := newMockURIFinder()
		nodeAddrA, mockNodeServerA, closerA := setupNodeServer()
		nodeAddrB, mockNodeServerB, closerB := setupNodeServer()

		msgA := raftpb.Message{
			To: 99,
		}

		msgB := raftpb.Message{
			To: 101,
		}

		return TE{
			T:               t,
			msgs:            []raftpb.Message{msgA, msgB},
			mockURIFinder:   mockURIFinder,
			emitter:         router.NewEmitter("some-buffer", mockURIFinder),
			mockNodeServers: []*mockNodeServer{mockNodeServerA, mockNodeServerB},
			nodeAddrs:       []string{nodeAddrA, nodeAddrB},
			closers:         []io.Closer{closerA, closerB},
		}
	})

	o.AfterEach(func(t TE) {
		for _, closer := range t.closers {
			closer.Close()
		}
	})

	o.Group("when URI Finder does not return an error", func() {
		o.BeforeEach(func(t TE) TE {
			for _, addr := range t.nodeAddrs {
				t.mockURIFinder.FromIDOutput.Ret0 <- addr
			}
			close(t.mockURIFinder.FromIDOutput.Ret1)
			return t
		})

		o.Group("when node does not return an error", func() {
			o.BeforeEach(func(t TE) TE {
				for _, nodeServer := range t.mockNodeServers {
					nodeServer.UpdateOutput.Ret0 <- new(intra.UpdateResponse)
					close(nodeServer.UpdateOutput.Ret1)
				}
				return t
			})

			o.Spec("it uses the URI finder to find the correct node", func(t TE) {
				err := t.emitter.Emit(t.msgs[0])
				Expect(t, err == nil).To(BeTrue())

				Expect(t, t.mockURIFinder.FromIDInput.ID).To(ViaPolling(
					Chain(Receive(), Equal(uint64(99))),
				))
			})

			o.Spec("it only sends once", func(t TE) {
				err := t.emitter.Emit(t.msgs[0])
				Expect(t, err == nil).To(BeTrue())

				expectedUpdate := &intra.UpdateMessage{
					Name:     "some-buffer",
					Messages: []*raftpb.Message{&t.msgs[0]},
				}

				Expect(t, t.mockNodeServers[0].UpdateInput.Arg1).To(ViaPolling(
					Chain(Receive(), Equal(expectedUpdate)),
				))

				Expect(t, t.mockNodeServers[0].UpdateCalled).To(Always(HaveLen(1)))
			})

			o.Spec("it sends each message to the correct node", func(t TE) {
				err := t.emitter.Emit(t.msgs...)
				Expect(t, err == nil).To(BeTrue())

				expectedUpdateA := &intra.UpdateMessage{
					Name:     "some-buffer",
					Messages: []*raftpb.Message{&t.msgs[0]},
				}

				expectedUpdateB := &intra.UpdateMessage{
					Name:     "some-buffer",
					Messages: []*raftpb.Message{&t.msgs[1]},
				}

				var actualA, actualB *intra.UpdateMessage
				Expect(t, t.mockNodeServers[0].UpdateInput.Arg1).To(ViaPolling(
					Chain(Receive(), Or(Equal(expectedUpdateA), Equal(expectedUpdateB)), Fetch(&actualA)),
				))

				Expect(t, t.mockNodeServers[1].UpdateInput.Arg1).To(ViaPolling(
					Chain(Receive(), Or(Equal(expectedUpdateA), Equal(expectedUpdateB)), Fetch(&actualB)),
				))

				Expect(t, actualA).To(Not(Equal(actualB)))

				Expect(t, t.mockNodeServers[0].UpdateCalled).To(Always(HaveLen(1)))
				Expect(t, t.mockNodeServers[1].UpdateCalled).To(Always(HaveLen(1)))
			})
		})
	})

	o.Group("when URI Finder returns an error", func() {
		o.BeforeEach(func(t TE) TE {
			close(t.mockURIFinder.FromIDOutput.Ret0)
			t.mockURIFinder.FromIDOutput.Ret1 <- fmt.Errorf("some-error")
			return t
		})

		o.Spec("it returns an error", func(t TE) {
			err := t.emitter.Emit(t.msgs...)
			Expect(t, err == nil).To(BeFalse())
		})
	})
}

func setupNodeServer() (string, *mockNodeServer, io.Closer) {
	mockNodeServer := newMockNodeServer()
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	intra.RegisterNodeServer(s, mockNodeServer)
	go s.Serve(lis)

	return lis.Addr().String(), mockNodeServer, lis
}
