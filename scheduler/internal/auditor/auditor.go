package auditor

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb/intra"
)

type Node interface {
	Status(ctx context.Context, req *intra.StatusRequest, opts ...grpc.CallOption) (*intra.StatusResponse, error)
	Create(ctx context.Context, req *intra.CreateInfo, opts ...grpc.CallOption) (*intra.CreateResponse, error)
}

type Auditor struct {
	nodes []Node
}

func Start(poll time.Duration, nodes ...Node) *Auditor {
	a := &Auditor{
		nodes: nodes,
	}

	go a.run(poll)

	return a
}

func (a *Auditor) run(poll time.Duration) {
	for range time.Tick(poll) {
		var allIDs []uint64
		buffers := make(map[string][]uint64)
		nodes := make(map[uint64]Node)

		for _, node := range a.nodes {
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

		fmt.Println(buffers, a.nodes)

		for bufName, ids := range buffers {
			if len(ids) == 3 {
				continue
			}

			id, ok := a.findRandExcluded(ids, allIDs)
			if !ok {
				log.Printf("unable to find a new node for %s", bufName)
				continue
			}

			_, err := nodes[id].Create(context.Background(), &intra.CreateInfo{
				Name:  bufName,
				Peers: a.buildPeerInfos(id, ids),
			})

			if err != nil {
				log.Printf("Failed to add node (%d) to buffer cluster (%s): %s", id, bufName, err)
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
