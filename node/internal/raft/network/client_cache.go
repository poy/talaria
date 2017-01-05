package network

import (
	"log"
	"sync"

	"google.golang.org/grpc"

	"github.com/apoydence/talaria/pb/intra"
)

type ClientCache struct {
	lock    sync.Mutex
	clients map[string]intra.NodeRaftClient
}

func NewClientCache() *ClientCache {
	return &ClientCache{
		clients: make(map[string]intra.NodeRaftClient),
	}
}

func (c *ClientCache) Fetch(target string) intra.NodeRaftClient {
	c.lock.Lock()
	defer c.lock.Unlock()

	client, ok := c.clients[target]
	if !ok {
		client = c.establishClient(target)
		c.clients[target] = client
	}

	return client
}

func (c *ClientCache) establishClient(target string) intra.NodeRaftClient {
	conn, err := grpc.Dial(target, grpc.WithInsecure())
	if err != nil {
		log.Printf("did not connect to node raft server: %s", err)
		return nil
	}
	return intra.NewNodeRaftClient(conn)
}
