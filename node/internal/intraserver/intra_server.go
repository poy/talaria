package intraserver

import (
	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb/intra"
)

type IOFetcher interface {
	Create(name string) error
}

type IntraServer struct {
	fetcher IOFetcher
}

func New(fetcher IOFetcher) *IntraServer {
	return &IntraServer{
		fetcher: fetcher,
	}
}

func (s *IntraServer) Create(ctx context.Context, info *intra.CreateInfo) (*intra.CreateResponse, error) {
	return new(intra.CreateResponse), s.fetcher.Create(info.Name)
}
