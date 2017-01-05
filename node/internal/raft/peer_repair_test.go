package raft_test

import (
	"testing"
	"time"

	"github.com/apoydence/eachers/testhelpers"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"

	"github.com/apoydence/talaria/node/internal/raft"
)

type TPR struct {
	*testing.T
	repair        *raft.PeerRepair
	mockPeerStore *mockPeerStore
	mockNode      *mockNode
}

func TestPeerRepair(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TPR {
		mockNode := newMockNode()
		mockPeerStore := newMockPeerStore()

		testhelpers.AlwaysReturn(mockPeerStore.PeersOutput.Ret0, []string{"A", "B", "C"})

		close(mockPeerStore.PeersOutput.Ret1)
		close(mockPeerStore.SetPeersOutput.Ret0)
		close(mockNode.AddPeerOutput.Ret0)
		close(mockNode.RemovePeerOutput.Ret0)

		return TPR{
			T:             t,
			mockNode:      mockNode,
			mockPeerStore: mockPeerStore,
			repair:        raft.NewPeerRepair(mockPeerStore, mockNode),
		}
	})

	o.Spec("it sets the peer store with the actual", func(t TPR) {
		actual := []string{"A", "B", "D"}
		t.repair.SetExpectedPeers(actual)
		Expect(t, t.mockPeerStore.SetPeersInput.Arg0).To(ViaPollingMatcher{
			Duration: 3 * time.Second,
			Matcher:  Chain(Receive(), Equal(actual)),
		})
	})

	o.Spec("it removes the dead peer from the node", func(t TPR) {
		actual := []string{"A", "B", "D"}
		t.repair.SetExpectedPeers(actual)
		Expect(t, t.mockNode.RemovePeerInput.Peer).To(ViaPollingMatcher{
			Duration: 3 * time.Second,
			Matcher:  Chain(Receive(), Equal("C")),
		})
	})

	o.Spec("it adds the new peer to the node", func(t TPR) {
		actual := []string{"A", "B", "D"}
		t.repair.SetExpectedPeers(actual)
		Expect(t, t.mockNode.AddPeerInput.Peer).To(ViaPollingMatcher{
			Duration: 3 * time.Second,
			Matcher:  Chain(Receive(), Equal("D")),
		})
	})

}
