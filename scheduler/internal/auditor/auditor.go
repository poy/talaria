package auditor

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/poy/talaria/api/intra"
	pb "github.com/poy/talaria/api/v1"
)

type Node interface {
	Status(ctx context.Context, req *intra.StatusRequest, opts ...grpc.CallOption) (*intra.StatusResponse, error)
	Create(ctx context.Context, req *intra.CreateInfo, opts ...grpc.CallOption) (*intra.CreateResponse, error)
	UpdateConfig(ctx context.Context, req *intra.UpdateConfigRequest, opts ...grpc.CallOption) (*intra.UpdateConfigResponse, error)
	Leader(ctx context.Context, req *intra.LeaderRequest, opts ...grpc.CallOption) (*intra.LeaderResponse, error)
}

type Auditor struct {
	nodes    map[Node]string
	nodeURIs map[string]Node

	mu       sync.RWMutex
	listResp pb.ListResponse
}

func Start(poll time.Duration, nodes map[Node]string) *Auditor {
	a := &Auditor{
		nodes: nodes,
	}

	a.nodeURIs = a.buildURIs(nodes)

	go a.run(poll)

	return a
}

func (a *Auditor) List() pb.ListResponse {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.listResp
}

func (a *Auditor) run(poll time.Duration) {
	for range time.Tick(poll) {
		log.Println("Running audit...")
		buffers := make(map[string][]string)
		externalAddrs := make(map[string]string)
		nodePeers := make(map[Node][]*intra.StatusBufferInfo)

		for node, _ := range a.nodes {
			resp, err := node.Status(context.Background(), new(intra.StatusRequest))
			if err != nil {
				log.Printf("unable to query node: %s", err)
				continue
			}

			URI := a.nodes[node]
			externalAddrs[URI] = resp.ExternalAddr

			nodePeers[node] = resp.Buffers
			for _, bufName := range resp.Buffers {
				buffers[bufName.Name] = append(buffers[bufName.Name], URI)
			}
		}

		a.setClusterInfo(buffers, externalAddrs)
		a.addMissingNode(nodePeers, buffers)
		a.fixMissingPeers(nodePeers, buffers)
	}
}

func (a *Auditor) buildURIs(m map[Node]string) map[string]Node {
	uris := make(map[string]Node)
	for k, v := range m {
		uris[v] = k
	}
	return uris
}

func (a *Auditor) setClusterInfo(buffers map[string][]string, externalAddrs map[string]string) {
	log.Println("Saving cluster info...")
	defer log.Println("Done saving cluster info.")

	var results []*pb.ClusterInfo
	for name, addrs := range buffers {
		log.Printf("Saving results for %s", name)
		info := &pb.ClusterInfo{
			Name:   string(name),
			Leader: a.fetchLeader(name, addrs, externalAddrs),
			Nodes:  a.buildNodeInfoList(addrs),
		}
		results = append(results, info)
		log.Printf("Results for %s: %v", name, info)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.listResp = pb.ListResponse{
		Info: results,
	}
}

func (a *Auditor) fetchLeader(name string, addrs []string, externalAddrs map[string]string) string {
	n := a.nodeURIs[addrs[rand.Intn(len(addrs))]]
	if n == nil {
		return ""
	}

	log.Printf("Fetching leader for %s...", name)
	resp, err := n.Leader(context.Background(), &intra.LeaderRequest{Name: name})
	if err != nil {
		log.Printf("Failed to fetch leader for %s: %s", name, err)
		return ""
	}

	leader := externalAddrs[resp.Addr]
	log.Printf("Leader for %s is (intra %s) %s", name, resp.Addr, leader)

	return leader
}

func (a *Auditor) buildNodeInfoList(addrs []string) []*pb.NodeInfo {
	var results []*pb.NodeInfo
	for _, addr := range addrs {
		results = append(results, &pb.NodeInfo{
			URI: addr,
		})
	}
	return results
}

// addMissingNode adds a node to a buffer that does not have 3 nodes servicing it
func (a *Auditor) addMissingNode(actuals map[Node][]*intra.StatusBufferInfo, buffers map[string][]string) {
	for name, addrs := range buffers {
		if len(addrs) == 3 {
			continue
		}

		log.Printf("Buffer %s only has %d nodes", name, len(addrs))

		newAddr, newNode, ok := a.findRandExcluded(addrs)
		if !ok {
			log.Printf("unable to find a new node for %s", name)
			continue
		}

		log.Printf("Adding %s to buffer %s...", newAddr, name)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := newNode.Create(ctx, &intra.CreateInfo{
			Name:  name,
			Peers: a.buildPeerInfos(newAddr, addrs),
		})

		if err != nil {
			log.Printf("Failed to add node %s to buffer %s: %s", newAddr, name, err)
			continue
		}

		newExpected := append([]string{newAddr}, addrs...)

		for _, addr := range addrs {
			node, ok := a.nodeURIs[addr]
			if !ok {
				log.Printf("Unknown addr %s. Skipping...", addr)
				continue
			}

			log.Printf("Adding %s to buffer %s", newAddr, name)

			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := node.UpdateConfig(ctx, &intra.UpdateConfigRequest{
				Name:          name,
				ExpectedNodes: newExpected,
			})

			if err != nil {
				log.Printf("Failed to add node %s to buffer %s: %s", newAddr, name, err)
				continue
			}
		}
	}
}

// fixMissingPeers sets the expected peers to nodes who have the wrong expected
// peers based on the other nodes.
func (a *Auditor) fixMissingPeers(actuals map[Node][]*intra.StatusBufferInfo, buffers map[string][]string) {
	for node, infos := range actuals {
		for _, info := range infos {
			actual := buffers[info.Name]
			if len(info.ExpectedNodes) == 3 || len(actual) != 3 {
				continue
			}

			log.Printf("Repairing expected peers for buffer %s...", info.Name)
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := node.UpdateConfig(ctx, &intra.UpdateConfigRequest{
				Name:          info.Name,
				ExpectedNodes: actual,
			})

			if err != nil {
				log.Printf("Failed to repair expected nodes to buffer cluster (%s): %s", info.Name, err)
				continue
			}
		}
	}

}

func (a *Auditor) buildPeerInfos(newAddr string, addrs []string) []*intra.PeerInfo {
	result := []*intra.PeerInfo{{Addr: newAddr}}
	for _, addr := range addrs {
		result = append(result, &intra.PeerInfo{Addr: addr})
	}
	return result
}

func (a *Auditor) findRandExcluded(included []string) (string, Node, bool) {
	for URI, node := range a.nodeURIs {
		if !a.contains(URI, included) {
			return URI, node, true
		}
	}
	return "", nil, false
}

func (a *Auditor) contains(x string, j []string) bool {
	for _, i := range j {
		if i == x {
			return true
		}
	}

	return false
}
