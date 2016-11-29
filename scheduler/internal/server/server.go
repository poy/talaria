package server

import (
	"fmt"
	"log"
	"math/rand"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
)

type NodeFetcher interface {
	FetchNodes() (client []intra.NodeClient)
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

	nodes := s.fetcher.FetchNodes()
	if len(nodes) == 0 {
		return nil, fmt.Errorf("No nodes available")
	}

	for _, node := range nodes {
		if _, err = node.Create(ctx, info); err != nil {
			log.Printf("Error scheduling %s", info.Name)
			return s.retryableCreate(ctx, info, count+1, err)
		}
	}

	seed := rand.Intn(1000)
	for i := range nodes {
		leaderInfo, err := nodes[(i+seed)%len(nodes)].Leader(ctx, &intra.LeaderRequest{})
		if err != nil {
			log.Printf("Error fetching leader information: %s", err)
			continue
		}

		return &pb.CreateResponse{
			Uri: leaderInfo.Peer.Uri,
		}, nil
	}

	return nil, fmt.Errorf("unable to fetch the leader")
}
