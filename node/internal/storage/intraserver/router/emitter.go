package router

import (
	"context"
	"fmt"
	"io"
	"log"

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
	clients    map[uint64]connInfo
}

type connInfo struct {
	client intra.NodeClient
	closer io.Closer
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
		}, 0)

		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Emitter) sendUpdate(id uint64, msg *intra.UpdateMessage, attempt int) error {
	if attempt >= 5 {
		return fmt.Errorf("failed to send update")
	}

	client, err := e.fetchClient(id)
	if err != nil {
		return err
	}

	resp, err := client.Update(context.Background(), msg)

	if err != nil || resp.Code == intra.UpdateResponse_RetryFailure {
		log.Printf("Error sending message: %s", err)
		delete(e.clients, id)
		return e.sendUpdate(id, msg, attempt+1)
	}

	if resp.Code == intra.UpdateResponse_InvalidID {
		return fmt.Errorf("invalid ID")
	}

	if resp.Code == intra.UpdateResponse_InvalidBuffer {
		return fmt.Errorf("invalid buffer")
	}

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

func (e *Emitter) fetchClient(id uint64) (intra.NodeClient, error) {
	client, ok := e.clients[id]
	if ok {
		return client.client, nil
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

	c := intra.NewNodeClient(conn)
	e.clients[id] = connInfo{
		client: c,
		closer: conn,
	}

	return c, nil
}
