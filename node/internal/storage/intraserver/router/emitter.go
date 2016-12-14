package router

import (
	"context"
	"io"
	"log"
	"sync"

	"github.com/apoydence/talaria/pb/intra"
	"github.com/coreos/etcd/raft/raftpb"
	"google.golang.org/grpc"
)

type URIFinder interface {
	FromID(ID uint64) (string, error)
}

type Emitter struct {
	bufferName string
	uriFinder  URIFinder

	mu      sync.Mutex
	clients map[uint64]connInfo
}

type connInfo struct {
	client intra.NodeClient
	closer io.Closer
	diode  *OneToOne
}

func NewEmitter(bufferName string, uriFinder URIFinder) *Emitter {
	return &Emitter{
		bufferName: bufferName,
		uriFinder:  uriFinder,
		clients:    make(map[uint64]connInfo),
	}
}

func (e *Emitter) Emit(msgs ...raftpb.Message) error {
	for id, msgs := range e.organizeMessages(msgs) {
		err := e.sendUpdate(id, &intra.UpdateMessage{
			Name:     e.bufferName,
			Messages: msgs,
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Emitter) sendUpdate(id uint64, msg *intra.UpdateMessage) error {
	client, err := e.fetchClient(id)
	if err != nil {
		return err
	}

	client.Set(msg)
	return nil
}

func (e *Emitter) organizeMessages(msgs []raftpb.Message) map[uint64][]*raftpb.Message {
	m := make(map[uint64][]*raftpb.Message)

	for i := range msgs {
		msg := msgs[i]
		m[msg.To] = append(m[msg.To], &msg)
	}

	return m
}

func (e *Emitter) fetchClient(id uint64) (*OneToOne, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	client, ok := e.clients[id]
	if ok {
		return client.diode, nil
	}

	uri, err := e.uriFinder.FromID(id)
	if err != nil {
		log.Printf("Error fetching ID %d: %s", id, err)
		return nil, err
	}

	conn, err := grpc.Dial(uri, grpc.WithInsecure())
	if err != nil {
		// This can't be set until we do TLS
		log.Panic(err)
	}

	diode := NewOneToOne(100, AlerterFunc(func(missed int) {
		log.Printf("Emitting to %d has missed %d messages", id, missed)
	}))
	c := intra.NewNodeClient(conn)

	go e.serveDiode(id, diode, c)

	e.clients[id] = connInfo{
		client: c,
		closer: conn,
		diode:  diode,
	}

	return diode, nil
}

func (e *Emitter) serveDiode(id uint64, diode *OneToOne, c intra.NodeClient) {
	defer func() {
		e.mu.Lock()
		delete(e.clients, id)
		e.mu.Unlock()
	}()

	for {
		msg := diode.Next()

		_, err := c.Update(context.Background(), msg)

		if err != nil {
			log.Printf("Error sending message: %s", err)
			return
		}
	}
}
