package intraserver

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb/intra"
	"github.com/coreos/etcd/raft/raftpb"
)

type IOFetcher interface {
	Create(name string, peers []*intra.PeerInfo) error
	Leader(name string) (id uint64, err error)
	UpdateConfig(name string, change raftpb.ConfChange) error
	List() []string
}

type Router interface {
	Route(bufferName string, msgs []raftpb.Message)
}

type IntraServer struct {
	id      uint64
	fetcher IOFetcher
	router  Router
}

func New(ID uint64, fetcher IOFetcher, router Router) *IntraServer {
	return &IntraServer{
		id:      ID,
		fetcher: fetcher,
		router:  router,
	}
}

func (s *IntraServer) Create(ctx context.Context, info *intra.CreateInfo) (*intra.CreateResponse, error) {
	return new(intra.CreateResponse), s.fetcher.Create(info.Name, info.Peers)
}

func (s *IntraServer) Leader(ctx context.Context, request *intra.LeaderRequest) (*intra.LeaderInfo, error) {
	id, err := s.fetcher.Leader(request.Name)
	if err != nil {
		return nil, err
	}

	return &intra.LeaderInfo{
		Id: id,
	}, nil
}

func (s *IntraServer) Update(ctx context.Context, msg *intra.UpdateMessage) (*intra.UpdateResponse, error) {
	msgs, err := s.validateMessages(msg.Messages)
	if err != nil {
		return &intra.UpdateResponse{
			Code: intra.UpdateResponse_InvalidID,
		}, nil
	}

	s.router.Route(msg.Name, msgs)
	return &intra.UpdateResponse{
		Code: intra.UpdateResponse_Success,
	}, nil
}

func (s *IntraServer) UpdateConfig(ctx context.Context, msg *intra.UpdateConfigRequest) (*intra.UpdateConfigResponse, error) {
	err := s.fetcher.UpdateConfig(msg.Name, *msg.Change)
	return new(intra.UpdateConfigResponse), err
}

func (s *IntraServer) Status(ctx context.Context, req *intra.StatusRequest) (*intra.StatusResponse, error) {
	return &intra.StatusResponse{
		Id:      s.id,
		Buffers: s.fetcher.List(),
	}, nil
}

func (s *IntraServer) validateMessages(msgs []*raftpb.Message) ([]raftpb.Message, error) {
	var result []raftpb.Message

	for _, msg := range msgs {
		if s.id != msg.To {
			return nil, fmt.Errorf("invalid ID %d (expected %d)", msg.To, s.id)
		}
		result = append(result, *msg)
	}

	return result, nil
}
