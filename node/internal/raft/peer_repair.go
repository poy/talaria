package raft

import (
	"log"
	"sync"
	"time"

	rafthashi "github.com/hashicorp/raft"
)

type PeerStore interface {
	rafthashi.PeerStore
}

type Node interface {
	AddPeer(peer string) rafthashi.Future
	RemovePeer(peer string) rafthashi.Future
}

type PeerRepair struct {
	peerStore rafthashi.PeerStore
	node      Node

	lock     sync.Mutex
	expected []string
}

func NewPeerRepair(store rafthashi.PeerStore, node Node) *PeerRepair {
	expected, _ := store.Peers()

	r := &PeerRepair{
		peerStore: store,
		node:      node,
		expected:  expected,
	}
	go r.run()

	return r
}

func (r *PeerRepair) SetExpectedPeers(peers []string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.expected = peers
}

func (r *PeerRepair) ExpectedPeers() (peers []string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.expected
}

func (r *PeerRepair) run() {
	for range time.Tick(time.Second) {
		r.lock.Lock()
		expected := r.expected
		r.lock.Unlock()

		actual, _ := r.peerStore.Peers()
		add, remove := r.delta(actual, expected)
		for _, peer := range remove {
			log.Printf("Repair: Remove %s (expected=%v actual=%v)", peer, expected, actual)
			r.node.RemovePeer(peer)
		}

		for _, peer := range add {
			log.Printf("Repair: Add %s (expected=%v actual=%v)", peer, expected, actual)
			r.node.AddPeer(peer)
		}
		r.peerStore.SetPeers(expected)
	}
}

func (r *PeerRepair) delta(actual, expected []string) (add, remove []string) {
	all := make([]string, len(actual))
	copy(all, actual)

	for _, e := range expected {
		if idx, ok := r.contains(e, all); ok {
			all = append(all[:idx], all[idx+1:]...)
			continue
		}

		add = append(add, e)
	}

	return add, all
}

func (r *PeerRepair) contains(a string, b []string) (int, bool) {
	for i, s := range b {
		if s == a {
			return i, true
		}
	}
	return -1, false
}
