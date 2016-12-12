package intraserver

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb/intra"
	"github.com/apoydence/talaria/scheduler/internal/server"
)

type NodeFetcher interface {
	FetchAllNodes() []server.NodeInfo
}

type IntraServer struct {
	fetcher NodeFetcher
}

func New(fetcher NodeFetcher) *IntraServer {
	return &IntraServer{
		fetcher: fetcher,
	}
}

func (s *IntraServer) FromID(ctx context.Context, in *intra.FromIdRequest) (*intra.FromIdResponse, error) {
	nodes := s.fetcher.FetchAllNodes()
	for _, n := range nodes {
		if n.ID == in.Id {
			return &intra.FromIdResponse{
				Uri: n.URI,
			}, nil
		}
	}

	return nil, fmt.Errorf("unknown ID: %d", in.Id)
}
