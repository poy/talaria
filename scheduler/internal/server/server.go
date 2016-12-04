package server

import (
	"fmt"
	"log"
	"math/rand"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
)

type NodeClient interface {
	Create(ctx context.Context, in *intra.CreateInfo, opts ...grpc.CallOption) (*intra.CreateResponse, error)
	Leader(ctx context.Context, in *intra.LeaderRequest, opts ...grpc.CallOption) (*intra.LeaderInfo, error)
	Update(ctx context.Context, in *intra.UpdateMessage, opts ...grpc.CallOption) (*intra.UpdateResponse, error)
	Status(ctx context.Context, in *intra.StatusRequest, opts ...grpc.CallOption) (*intra.StatusResponse, error)
}

type NodeInfo struct {
	Client intra.NodeClient
	URI    string
	ID     uint64
}

type NodeFetcher interface {
	FetchNodes() (client []NodeInfo)
}

type Server struct {
	fetcher NodeFetcher
}

func New(fetcher NodeFetcher) *Server {
	return &Server{
		fetcher: fetcher,
	}
}

func (s *Server) Create(ctx context.Context, info *pb.CreateInfo) (*pb.CreateResponse, error) {
	log.Printf("Scheduling %s", info.Name)
	return s.retryableCreate(ctx, &intra.CreateInfo{Name: info.Name}, 0, nil)
}

func (s *Server) retryableCreate(ctx context.Context, info *intra.CreateInfo, count int, err error) (*pb.CreateResponse, error) {
	if count >= 5 {
		return nil, err
	}

	// TODO: If some nodes fail and others don't, we don't need a full set of new
	// nodes on next request
	nodes := s.fetcher.FetchNodes()
	if len(nodes) == 0 {
		return nil, fmt.Errorf("No nodes available")
	}

	for _, node := range nodes {
		log.Printf("Creating buffer %s on node %s (ID=%d)", info.Name, node.URI, node.ID)
		if _, err = node.Client.Create(ctx, info); err != nil {
			log.Printf("Error scheduling %s on %s: %s", info.Name, node.URI, err)
			return s.retryableCreate(ctx, info, count+1, err)
		}
	}

	seed := rand.Intn(1000)
	for i := range nodes {
		log.Printf("Fetching leader for %s", info.Name)
		leaderInfo, err := nodes[(i+seed)%len(nodes)].Client.Leader(ctx, &intra.LeaderRequest{
			Name: info.Name,
		})
		if err != nil {
			log.Printf("Error fetching leader information: %s", err)
			continue
		}

		leader, found := s.findViaID(leaderInfo.Id, nodes)
		if !found {
			return nil, fmt.Errorf("unknown leader ID: %d", leaderInfo.Id)
		}
		log.Printf("Found leader for %s: %s (ID=%d)", info.Name, leader.URI, leader.ID)

		return &pb.CreateResponse{
			Uri: leader.URI,
		}, nil
	}

	return nil, fmt.Errorf("unable to fetch the leader")
}

func (s *Server) findViaID(ID uint64, nodes []NodeInfo) (NodeInfo, bool) {
	for _, n := range nodes {
		if ID == n.ID {
			return n, true
		}
	}

	return NodeInfo{}, false
}
