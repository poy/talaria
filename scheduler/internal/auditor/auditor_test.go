//go:generate hel

package auditor_test

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/poy/eachers/testhelpers"
	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
	"github.com/poy/talaria/api/intra"
	pb "github.com/poy/talaria/api/v1"
	"github.com/poy/talaria/scheduler/internal/auditor"
)

func TestMain(m *testing.M) {
	flag.Parse()
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
			a:         auditor.Start(time.Millisecond, castMockNodes(mockNodes)),
			mockNodes: mockNodes,
		}
	})

	o.Group("when all the nodes do not return an error", func() {
		o.BeforeEach(func(t TT) TT {
			for _, n := range t.mockNodes {
				close(n.StatusOutput.Ret1)

				testhelpers.AlwaysReturn(n.CreateOutput.Ret0, new(intra.CreateResponse))
				close(n.CreateOutput.Ret1)

				testhelpers.AlwaysReturn(n.LeaderOutput.Ret0, &intra.LeaderResponse{})
				close(n.LeaderOutput.Ret1)

				testhelpers.AlwaysReturn(n.UpdateConfigOutput.Ret0, new(intra.UpdateConfigResponse))
				close(n.UpdateConfigOutput.Ret1)
			}
			return t
		})

		o.Group("when a buffer does not have 3 nodes", func() {
			o.BeforeEach(func(t TT) TT {
				testhelpers.AlwaysReturn(t.mockNodes[0].StatusOutput.Ret0, &intra.StatusResponse{
					Buffers: []*intra.StatusBufferInfo{{Name: "good", ExpectedNodes: []string{"0", "1", "2"}}},
				})

				for _, n := range t.mockNodes[1:] {
					testhelpers.AlwaysReturn(n.StatusOutput.Ret0, &intra.StatusResponse{
						Buffers: []*intra.StatusBufferInfo{
							{Name: "good", ExpectedNodes: []string{"0", "1", "2"}},
							{Name: "standalone", ExpectedNodes: []string{"1", "2"}}},
					})
				}
				return t
			})

			o.Spec("a buffer is created on left out node", func(t TT) {
				Expect(t, t.mockNodes[0].CreateInput.Req).To(ViaPolling(
					Chain(Receive(), Equal(&intra.CreateInfo{
						Name: "standalone",
						Peers: []*intra.PeerInfo{
							{"0"}, {"1"}, {"2"},
						},
					})),
				))

				Expect(t, t.mockNodes[1].UpdateConfigInput.Req).To(ViaPolling(
					Chain(Receive(), Equal(&intra.UpdateConfigRequest{
						Name:          "standalone",
						ExpectedNodes: []string{"0", "1", "2"},
					})),
				))
			})

			o.Spec("a buffer is not created on attending nodes", func(t TT) {
				Expect(t, t.mockNodes[0].UpdateConfigInput.Req).To(Always(Not(Receive())))
				Expect(t, t.mockNodes[1].CreateInput.Req).To(Always(Not(Receive())))
				Expect(t, t.mockNodes[2].CreateInput.Req).To(Always(Not(Receive())))
			})
		})

		o.Group("when a buffer does not know about a peer", func() {
			o.BeforeEach(func(t TT) TT {
				testhelpers.AlwaysReturn(t.mockNodes[0].StatusOutput.Ret0, &intra.StatusResponse{
					Buffers: []*intra.StatusBufferInfo{
						{Name: "good", ExpectedNodes: []string{"0", "1", "2"}},
						{Name: "missing", ExpectedNodes: []string{"0", "1"}},
					},
				})

				for _, n := range t.mockNodes[1:] {
					testhelpers.AlwaysReturn(n.StatusOutput.Ret0, &intra.StatusResponse{
						Buffers: []*intra.StatusBufferInfo{
							{Name: "good", ExpectedNodes: []string{"0", "1", "2"}},
							{Name: "missing", ExpectedNodes: []string{"0", "1", "2"}},
						},
					})
				}
				return t
			})

			o.Spec("it updates the buffer to add the missing peer", func(t TT) {

				var req *intra.UpdateConfigRequest
				Expect(t, t.mockNodes[0].UpdateConfigInput.Req).To(ViaPolling(
					Chain(Receive(), Fetch(&req)),
				))

				Expect(t, req.Name).To(Equal("missing"))
			})
		})
	})
}

func TestAuditorWithDeadNodes(t *testing.T) {
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
			a:         auditor.Start(time.Millisecond, castMockNodes(mockNodes)),
			mockNodes: mockNodes,
		}
	})

	o.Group("when a buffer has a dead ID", func() {
		o.BeforeEach(func(t TT) TT {
			for _, n := range t.mockNodes {
				testhelpers.AlwaysReturn(n.StatusOutput.Ret0, &intra.StatusResponse{
					Buffers: []*intra.StatusBufferInfo{
						{
							Name:          "standalone",
							ExpectedNodes: []string{"0", "1", "2", "99"},
						},
					},
				})
				close(n.StatusOutput.Ret1)

				testhelpers.AlwaysReturn(n.LeaderOutput.Ret0, new(intra.LeaderResponse))
				close(n.LeaderOutput.Ret1)

				testhelpers.AlwaysReturn(n.UpdateConfigOutput.Ret0, new(intra.UpdateConfigResponse))
				close(n.UpdateConfigOutput.Ret1)
			}
			return t
		})

		o.Spec("remove the dead ID", func(t TT) {
			var req *intra.UpdateConfigRequest
			Expect(t, t.mockNodes[0].UpdateConfigInput.Req).To(ViaPolling(
				Chain(Receive(), Fetch(&req)),
			))

			Expect(t, req.Name).To(Equal("standalone"))
			Expect(t, req.ExpectedNodes).To(HaveLen(3))
		})
	})
}

func TestAuditorList(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		mockNodes := []*mockNode{
			newMockNode(),
			newMockNode(),
			newMockNode(),
		}

		for i, n := range mockNodes {
			testhelpers.AlwaysReturn(n.LeaderOutput.Ret0, &intra.LeaderResponse{Addr: "0"})
			close(n.LeaderOutput.Ret1)
			testhelpers.AlwaysReturn(n.StatusOutput.Ret0, &intra.StatusResponse{
				ExternalAddr: fmt.Sprintf("external-%d", i),
				Buffers:      []*intra.StatusBufferInfo{{Name: "good"}, {Name: "standalone"}},
			})
			close(n.StatusOutput.Ret1)
		}

		return TT{
			T:         t,
			a:         auditor.Start(time.Millisecond, castMockNodes(mockNodes)),
			mockNodes: mockNodes,
		}
	})

	o.Spec("it reports its last result set", func(t TT) {
		var list pb.ListResponse

		f := func() int {
			list = t.a.List()
			return len(list.Info)
		}
		Expect(t, f).To(ViaPolling(Equal(2)))

		Expect(t, list.Info[0].Name).To(Or(Equal("good"), Equal("standalone")))
		Expect(t, list.Info[1].Name).To(Or(Equal("good"), Equal("standalone")))
		Expect(t, list.Info[0].Name).To(Not(Equal(list.Info[1].Name)))
		Expect(t, list.Info[0].Leader).To(Equal("external-0"))
		Expect(t, list.Info[1].Leader).To(Equal("external-0"))
		Expect(t, list.Info[0].Nodes).To(Contain(
			&pb.NodeInfo{URI: "0"},
			&pb.NodeInfo{URI: "1"},
			&pb.NodeInfo{URI: "2"},
		))
		Expect(t, list.Info[1].Nodes).To(Contain(
			&pb.NodeInfo{URI: "0"},
			&pb.NodeInfo{URI: "1"},
			&pb.NodeInfo{URI: "2"},
		))
	})
}

func castMockNodes(n []*mockNode) map[auditor.Node]string {
	ns := make(map[auditor.Node]string)
	for i, nn := range n {
		ns[nn] = fmt.Sprintf("%d", i)
	}
	return ns
}
