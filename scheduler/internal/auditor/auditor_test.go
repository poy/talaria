//go:generate hel

package auditor_test

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/apoydence/eachers/testhelpers"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/apoydence/talaria/scheduler/internal/auditor"
)

func TestMain(m *testing.M) {
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}

	os.Exit(m.Run())
}

type TT struct {
	*testing.T
	mockNodes []*mockNode
	a         *auditor.Auditor
}

func TestAuditorWithEnoughNodes(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		mockNodes := []*mockNode{
			newMockNode(),
			newMockNode(),
			newMockNode(),
		}

		return TT{
			T:         t,
			a:         auditor.Start(time.Millisecond, castMockNodes(mockNodes)...),
			mockNodes: mockNodes,
		}
	})

	o.Group("when all the nodes do not return an error", func() {
		o.BeforeEach(func(t TT) TT {
			for _, n := range t.mockNodes {
				close(n.StatusOutput.Ret1)
			}
			return t
		})

		o.Group("when a buffer does not have 3 nodes", func() {
			o.BeforeEach(func(t TT) TT {
				testhelpers.AlwaysReturn(t.mockNodes[0].StatusOutput.Ret0, &intra.StatusResponse{
					Id:      uint64(0),
					Buffers: []string{"good"},
				})

				for i, n := range t.mockNodes[1:] {
					testhelpers.AlwaysReturn(n.StatusOutput.Ret0, &intra.StatusResponse{
						Id:      uint64(i + 1),
						Buffers: []string{"good", "standalone"},
					})
				}
				return t
			})

			o.Spec("a buffer is created on left out node", func(t TT) {
				Expect(t, t.mockNodes[0].CreateInput.Req).To(ViaPolling(
					Chain(Receive(), Equal(&intra.CreateInfo{
						Name: "standalone",
						Peers: []*intra.PeerInfo{
							{Id: 0}, {Id: 1}, {Id: 2},
						},
					})),
				))
			})

			o.Spec("a buffer is not created on attending nodes", func(t TT) {
				Expect(t, t.mockNodes[1].CreateInput.Req).To(Always(Not(Receive())))
				Expect(t, t.mockNodes[2].CreateInput.Req).To(Always(Not(Receive())))
			})
		})
	})
}

func TestAuditorWithoutEnoughNodes(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		mockNodes := []*mockNode{
			newMockNode(),
			newMockNode(),
		}

		return TT{
			T:         t,
			a:         auditor.Start(time.Millisecond, castMockNodes(mockNodes)...),
			mockNodes: mockNodes,
		}
	})

	o.Group("when a buffer does not have 3 nodes", func() {
		o.BeforeEach(func(t TT) TT {
			for i, n := range t.mockNodes {
				testhelpers.AlwaysReturn(n.StatusOutput.Ret0, &intra.StatusResponse{
					Id:      uint64(i),
					Buffers: []string{"standalone"},
				})
				close(n.StatusOutput.Ret1)

				testhelpers.AlwaysReturn(n.CreateOutput.Ret0, new(intra.CreateResponse))
				close(n.CreateOutput.Ret1)
			}
			return t
		})

		o.Spec("a buffer is not created on attending nodes", func(t TT) {
			Expect(t, t.mockNodes[0].CreateCalled).To(Always(HaveLen(0)))
			Expect(t, t.mockNodes[1].CreateCalled).To(Always(HaveLen(0)))
		})
	})
}

func castMockNodes(n []*mockNode) []auditor.Node {
	var ns []auditor.Node
	for _, nn := range n {
		ns = append(ns, nn)
	}
	return ns
}
