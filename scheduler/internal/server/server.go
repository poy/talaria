package server

import (
	"fmt"
	"log"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
)

type NodeFetcher interface {
	FetchNode() intra.NodeClient
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
		return new(pb.CreateResponse), err
	}

	node := s.fetcher.FetchNode()
	if node == nil {
		return nil, fmt.Errorf("No nodes available")
	}

	if _, err = node.Create(ctx, info); err != nil {
		log.Printf("Error scheduling %s", info.Name)
		return s.retryableCreate(ctx, info, count+1, err)
	}

	return new(pb.CreateResponse), nil
}
