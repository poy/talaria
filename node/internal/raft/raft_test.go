//go:generate hel

package raft_test

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
	"github.com/poy/talaria/api/stored"
	"github.com/poy/talaria/node/internal/raft"
	"github.com/poy/talaria/node/internal/raft/network"
	rafthashi "github.com/hashicorp/raft"
)

var testWriter io.Writer = os.Stderr

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		testWriter = ioutil.Discard
	}

	os.Exit(m.Run())
}

type TR struct {
	*testing.T
	schedulerHandler *network.SchedulerInbound
	inbounds         []*network.Inbound
	nodes            []*raft.Raft
	consumer         chan rafthashi.RPC
	nodeMap          map[string]*raft.Raft
}

func TestRaft(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TR {
		schedulerHandler := network.NewSchedulerInbound("some-addr", nil)

		var inbounds []*network.Inbound
		for i := 0; i < 3; i++ {
			inbounds = append(inbounds, network.NewInbound("localhost:0", schedulerHandler))
		}

		m := make(map[string]*raft.Raft)
		var nodes []*raft.Raft
		for i := 0; i < 3; i++ {
			node := startRaftNode(inbounds, i, "some-buffer")
			nodes = append(nodes, node)
			m[inbounds[i].Addr()] = node
		}

		f := func() bool {
			return nodes[0].Leader() == nodes[1].Leader() &&
				nodes[0].Leader() == nodes[2].Leader() &&
				nodes[0].Leader() != ""
		}

		Expect(t, f).To(ViaPollingMatcher{
			Duration: 10 * time.Second,
			Matcher:  BeTrue(),
		})

		return TR{
			T:        t,
			nodes:    nodes,
			nodeMap:  m,
			inbounds: inbounds,
		}
	})

	o.Spec("it stores data", func(t TR) {
		var leader *raft.Raft
		f := func() bool {
			leader = fetchLeader(t)
			return leader == nil
		}
		Expect(t, f).To(ViaPollingMatcher{
			Duration: 5 * time.Second,
			Matcher:  BeFalse(),
		})

		storedData := stored.Data{
			Type:    stored.Data_Normal,
			Payload: []byte("some-data"),
		}

		err := leader.Write(storedData, time.Second)
		Expect(t, err == nil).To(BeTrue())

		for i := range t.nodes {
			var entry []byte
			ff := func() bool {
				entry, _, _ = t.nodes[i].ReadAt(0)
				return entry != nil
			}

			Expect(t, ff).To(ViaPollingMatcher{
				Duration: 5 * time.Second,
				Matcher:  BeTrue(),
			})
			Expect(t, entry).To(Equal([]byte("some-data")))
		}
	})

	o.Group("when a node dies and comes back", func() {
		o.BeforeEach(func(t TR) TR {
			restartFirstNode(t, "some-buffer")

			return t
		})

		o.Spec("the cluster recovers", func(t TR) {
			var leader *raft.Raft
			f := func() bool {
				leader = fetchLeader(t)
				return leader == nil
			}
			Expect(t, f).To(ViaPollingMatcher{
				Duration: 5 * time.Second,
				Matcher:  BeFalse(),
			})

			storedData := stored.Data{
				Type:    stored.Data_Normal,
				Payload: []byte("some-data"),
			}

			err := leader.Write(storedData, time.Second)
			Expect(t, err == nil).To(BeTrue())

			for i := range t.nodes {
				var entry []byte
				ff := func() bool {
					entry, _, _ = t.nodes[i].ReadAt(0)
					return entry != nil
				}

				Expect(t, ff).To(ViaPollingMatcher{
					Duration: 5 * time.Second,
					Matcher:  BeTrue(),
				})
				Expect(t, entry).To(Equal([]byte("some-data")))
			}

		})
	})

	o.Group("when data has been written and a node dies", func() {
		o.BeforeEach(func(t TR) TR {
			for i := 0; i < 200; i++ {
				err := writeToLeader(t, []byte(fmt.Sprintf("some-data-%d", i)))
				Expect(t, err == nil).To(BeTrue())
			}

			restartFirstNode(t, "some-buffer")
			return t
		})

		o.Spec("the cluster recovers", func(t TR) {
			for i := 0; i < 100; i++ {
				var entry []byte
				ff := func() bool {
					entry, _, _ = t.nodes[0].ReadAt(uint64(i))
					if entry == nil {
						return false
					}
					return string(entry) == fmt.Sprintf("some-data-%d", i+100)
				}

				Expect(t, ff).To(ViaPollingMatcher{
					Duration: 5 * time.Second,
					Matcher:  BeTrue(),
				})
			}
		})
	})
}

func writeToLeader(t TR, data []byte) error {
	var leader *raft.Raft
	f := func() bool {
		leader = fetchLeader(t)
		return leader == nil
	}
	Expect(t, f).To(ViaPollingMatcher{
		Duration: 5 * time.Second,
		Matcher:  BeFalse(),
	})

	storedData := stored.Data{
		Type:    stored.Data_Normal,
		Payload: data,
	}

	return leader.Write(storedData, time.Second)
}

func restartFirstNode(t TR, bufferName string) {
	t.nodes[0].Shutdown()

	t.inbounds[0] = network.NewInbound("localhost:0", t.schedulerHandler)
	node := startRaftNode(t.inbounds, 0, bufferName)
	t.nodeMap[t.inbounds[0].Addr()] = node
	t.nodes[0] = node

	var newPeers []string
	for _, inbound := range t.inbounds {
		newPeers = append(newPeers, inbound.Addr())
	}

	for _, node := range t.nodes[1:] {
		node.SetExpectedPeers(newPeers)
	}
}

func fetchLeader(t TR) *raft.Raft {
	leaderName := t.nodes[0].Leader()
	return t.nodeMap[leaderName]
}

func startRaftNode(inbounds []*network.Inbound, index int, bufferName string) *raft.Raft {
	var peers []string
	for _, inbound := range inbounds {
		peers = append(peers, inbound.Addr())
	}

	node, err := raft.Build(bufferName, inbounds[index],
		raft.WithPeers(peers),
		raft.WithLogger(log.New(testWriter, fmt.Sprintf("[RAFT-%d]", index), log.LstdFlags)),
		raft.WithBufferSize(100),
	)
	if err != nil {
		panic(err)
	}

	return node
}
