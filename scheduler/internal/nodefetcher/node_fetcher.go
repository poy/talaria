package nodefetcher

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/apoydence/talaria/pb/intra"
	"github.com/apoydence/talaria/scheduler/internal/server"
	"google.golang.org/grpc"
)

type NodeFetcher struct {
	clients []clientInfo
}

type clientInfo struct {
	URI    string
	conn   *grpc.ClientConn
	client intra.NodeClient
}

func New(URIs []string) *NodeFetcher {
	var clients []clientInfo
	for _, URI := range URIs {
		conn, err := grpc.Dial(URI, grpc.WithInsecure())
		if err != nil {
			log.Panicf("Error connecting to %s: %s", URI, err)
		}
		client := intra.NewNodeClient(conn)
		clients = append(clients, clientInfo{
			conn:   conn,
			client: client,
			URI:    URI,
		})
	}

	return &NodeFetcher{
		clients: clients,
	}
}

func (f *NodeFetcher) FetchNodes() []server.NodeInfo {
	if len(f.clients) < 3 {
		return nil
	}

	var infos []server.NodeInfo
	for _, p := range rand.Perm(3) {
		infos = append(infos, server.NodeInfo{
			Client: f.clients[p].client,
			URI:    f.clients[p].URI,
			ID:     f.fetchID(f.clients[p], 0),
		})
	}

	return infos
}

func (f *NodeFetcher) fetchID(c clientInfo, attempt int) uint64 {
	if attempt >= 5 {
		log.Panicf("unable to fetch ID for %s", c.URI)
	}

	log.Printf("Fetchig status for node %s...", c.URI)
	resp, err := c.client.Status(context.Background(), new(intra.StatusRequest))
	if err != nil {
		time.Sleep(5 * time.Second)
		return f.fetchID(c, attempt+1)
	}
	log.Printf("Found ID (%d) for %s...", resp.Id, c.URI)
	return resp.Id
}
