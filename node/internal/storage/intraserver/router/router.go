package router

import (
	"log"
	"sync"

	"github.com/coreos/etcd/raft/raftpb"
)

type Router struct {
	mu     sync.Mutex
	routes map[string]chan raftpb.Message
}

func New() *Router {
	return &Router{
		routes: make(map[string]chan raftpb.Message),
	}
}

// This is invoked by the intra server handler
func (r *Router) Route(bufferName string, msgs []raftpb.Message) {
	c := r.fetchChannel(bufferName)
	for _, msg := range msgs {
		select {
		case c <- msg:
		default:
			log.Println("Buffer %s is not reading from the router fast enough", bufferName)
		}
	}
}

// This is invoked by the raft loop
func (r *Router) Receiver(bufferName string) func() (raftpb.Message, error) {
	c := r.fetchChannel(bufferName)
	return func() (raftpb.Message, error) {
		return <-c, nil
	}
}

func (r *Router) fetchChannel(bufferName string) chan raftpb.Message {
	r.mu.Lock()
	defer r.mu.Unlock()

	c, ok := r.routes[bufferName]
	if !ok {
		c = make(chan raftpb.Message, 100)
		r.routes[bufferName] = c
	}
	return c
}
