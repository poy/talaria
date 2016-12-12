package auditor

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/coreos/etcd/raft/raftpb"
)

type Node interface {
	Status(ctx context.Context, req *intra.StatusRequest, opts ...grpc.CallOption) (*intra.StatusResponse, error)
	Create(ctx context.Context, req *intra.CreateInfo, opts ...grpc.CallOption) (*intra.CreateResponse, error)
	UpdateConfig(ctx context.Context, req *intra.UpdateConfigRequest, opts ...grpc.CallOption) (*intra.UpdateConfigResponse, error)
	Leader(ctx context.Context, req *intra.LeaderRequest, opts ...grpc.CallOption) (*intra.LeaderInfo, error)
}

type Auditor struct {
	nodes map[Node]string

	mu       sync.RWMutex
	listResp pb.ListResponse
}

func Start(poll time.Duration, nodes map[Node]string) *Auditor {
	a := &Auditor{
		nodes: nodes,
	}

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
		var allIDs []uint64
		buffers := make(map[string][]uint64)
		nodes := make(map[uint64]Node)

		for node, _ := range a.nodes {
			resp, err := node.Status(context.Background(), new(intra.StatusRequest))
			if err != nil {
				log.Printf("unable to query node: %s", err)
				continue
			}

			allIDs = append(allIDs, resp.Id)
			nodes[resp.Id] = node

			for _, bufName := range resp.Buffers {
				buffers[bufName] = append(buffers[bufName], resp.Id)
			}
		}

		a.setClusterInfo(buffers, nodes)
		a.fixBuffers(buffers, allIDs, nodes)
	}
}

func (a *Auditor) setClusterInfo(buffers map[string][]uint64, nodes map[uint64]Node) {
	log.Println("Saving cluster info")
	var results []*pb.ClusterInfo
	for bufferName, ids := range buffers {
		log.Printf("Saving results for %s", bufferName)
		info := &pb.ClusterInfo{
			Name:   bufferName,
			Leader: a.fetchLeader(bufferName, ids, nodes),
			Nodes:  a.buildIPList(ids, nodes),
		}
		results = append(results, info)
		log.Printf("Results for %s: %v", bufferName, info)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.listResp = pb.ListResponse{
		Info: results,
	}
}

func (a *Auditor) fetchLeader(name string, ids []uint64, nodes map[uint64]Node) string {
	n := nodes[ids[uint64(rand.Intn(len(ids)))]]
	if n == nil {
		return ""
	}

	log.Printf("Fetching leader for %s...", name)
	resp, err := n.Leader(context.Background(), &intra.LeaderRequest{Name: name})
	if err != nil {
		log.Printf("Failed to fetch leader for %s: %s", name, err)
		return ""
	}
	log.Printf("Leader for %s is %d", name, resp.Id)

	return a.nodes[nodes[resp.Id]]
}

func (a *Auditor) buildIPList(ids []uint64, nodes map[uint64]Node) []string {
	var results []string
	for _, id := range ids {
		results = append(results, a.nodes[nodes[id]])
	}
	return results
}

func (a *Auditor) fixBuffers(buffers map[string][]uint64, allIDs []uint64, nodes map[uint64]Node) {
	for bufName, ids := range buffers {
		if len(ids) == 3 {
			continue
		}
		log.Printf("Buffer %s only has %d nodes...", bufName, len(ids))

		newId, ok := a.findRandExcluded(ids, allIDs)
		if !ok {
			log.Printf("unable to find a new node for %s", bufName)
			continue
		}

		log.Printf("Adding %d to buffer %s...", newId, bufName)
		_, err := nodes[newId].Create(context.Background(), &intra.CreateInfo{
			Name:  bufName,
			Peers: a.buildPeerInfos(newId, ids),
		})

		if err != nil {
			log.Printf("Failed to add node (%d) to buffer cluster (%s): %s", newId, bufName, err)
			continue
		}

		for _, id := range ids {
			log.Printf("Updating %d with new node %d", id, newId)
			_, err := nodes[id].UpdateConfig(context.Background(), &intra.UpdateConfigRequest{
				Name: bufName,
				Change: &raftpb.ConfChange{
					NodeID: newId,
					Type:   raftpb.ConfChangeAddNode,
				},
			})

			if err != nil {
				log.Printf("Failed to update node (%d) for buffer cluster (%s): %s", id, bufName, err)
				continue
			}
		}
	}

}

func (a *Auditor) buildPeerInfos(newId uint64, ids []uint64) []*intra.PeerInfo {
	result := []*intra.PeerInfo{{Id: newId}}
	for _, id := range ids {
		result = append(result, &intra.PeerInfo{Id: id})
	}
	return result
}

func (a *Auditor) findRandExcluded(included []uint64, ids []uint64) (uint64, bool) {
	seed := rand.Int()
	for i := range ids {
		idx := (i + seed) % len(ids)
		x := ids[idx]
		if !a.contains(x, included) {
			return x, true
		}
	}
	return 0, false
}

func (a *Auditor) contains(x uint64, j []uint64) bool {
	for _, i := range j {
		if i == x {
			return true
		}
	}

	return false
}
