package nodefetcher

import (
	"log"
	"math/rand"

	"github.com/apoydence/talaria/pb/intra"
	"google.golang.org/grpc"
)

type NodeFetcher struct {
	uris    []string
	clients []clientInfo
}

type clientInfo struct {
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
		})
	}

	return &NodeFetcher{
		uris:    URIs,
		clients: clients,
	}
}

func (f *NodeFetcher) FetchNode() intra.NodeClient {
	if len(f.clients) == 0 {
		return nil
	}

	return f.clients[rand.Intn(len(f.clients))].client
}
