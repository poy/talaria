package nodefetcher

import (
	"fmt"
	"log"
	"math/rand"

	"github.com/poy/talaria/api/intra"
	"github.com/poy/talaria/scheduler/internal/server"
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

func (f *NodeFetcher) FetchAllNodes() []server.NodeInfo {
	var results []server.NodeInfo
	for _, client := range f.clients {
		results = append(results, server.NodeInfo{
			Client: client.client,
			URI:    client.URI,
		})
	}
	return results
}

func (f *NodeFetcher) FetchNode(addr string) (node server.NodeInfo, err error) {
	for _, info := range f.clients {
		if info.URI == addr {
			return server.NodeInfo{Client: info.client, URI: addr}, nil
		}
	}

	return server.NodeInfo{}, fmt.Errorf("unable to find %s", addr)
}

func (f *NodeFetcher) FetchNodes(count int, exclude ...server.NodeInfo) []server.NodeInfo {
	if len(f.clients) < count {
		return nil
	}

	clients := f.exclude(exclude)

	num := f.permCount(len(clients), count)

	var infos []server.NodeInfo
	for _, p := range rand.Perm(num) {
		infos = append(infos, server.NodeInfo{
			Client: clients[p].client,
			URI:    clients[p].URI,
		})
	}

	return infos
}

func (f *NodeFetcher) permCount(max, count int) int {
	if max < count {
		return max
	}

	return count
}

func (f *NodeFetcher) exclude(exclude []server.NodeInfo) []clientInfo {
	var result []clientInfo
	for _, n := range f.clients {
		if !f.excluded(n, exclude) {
			result = append(result, n)
		}
	}

	return result
}

func (f *NodeFetcher) excluded(client clientInfo, exclude []server.NodeInfo) bool {
	for _, ex := range exclude {
		if ex.Client == client.client {
			return true
		}
	}
	return false
}
