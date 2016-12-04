package raftnode

import (
	"log"

	"github.com/apoydence/talaria/node/internal/storage/intraserver/router"
	"github.com/coreos/etcd/raft/raftpb"
)

type Receiver interface {
	Receiver(bufferName string) func() (raftpb.Message, error)
}

type NetworkWrapper struct {
	rx      func() (raftpb.Message, error)
	emitter *router.Emitter
}

func NewNetworkWrapper(bufferName string, receiver Receiver, emitter *router.Emitter) *NetworkWrapper {
	return &NetworkWrapper{
		rx:      receiver.Receiver(bufferName),
		emitter: emitter,
	}
}

func (w *NetworkWrapper) Recv() (raftpb.Message, error) {
	return w.rx()
}

func (w *NetworkWrapper) Emit(msgs []raftpb.Message) {
	if err := w.emitter.Emit(msgs...); err != nil {
		log.Printf("Failed to emit messages: %s", err)
	}
}
