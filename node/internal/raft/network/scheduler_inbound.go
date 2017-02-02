package network

import (
	"golang.org/x/net/context"

	"github.com/apoydence/talaria/api/intra"
)

type IOFetcher interface {
	Create(name string, peers []string) error
	ReadOnly(name string) error
	Leader(name string) (string, error)
	Status() map[string][]string
	SetExpectedPeers(name string, expectedPeers []string) error
}

type SchedulerInbound struct {
	ioFetcher    IOFetcher
	externalAddr string
}

func NewSchedulerInbound(externalAddr string, ioFetcher IOFetcher) *SchedulerInbound {
	return &SchedulerInbound{
		externalAddr: externalAddr,
		ioFetcher:    ioFetcher,
	}
}

func (i *SchedulerInbound) Create(ctx context.Context, in *intra.CreateInfo) (*intra.CreateResponse, error) {
	err := i.ioFetcher.Create(in.Name, i.convertPeers(in))
	if err != nil {
		return nil, err
	}

	return new(intra.CreateResponse), nil
}

func (i *SchedulerInbound) ReadOnly(ctx context.Context, in *intra.ReadOnlyInfo) (*intra.ReadOnlyResponse, error) {
	err := i.ioFetcher.ReadOnly(in.Name)
	if err != nil {
		return nil, err
	}

	return new(intra.ReadOnlyResponse), nil
}

func (i *SchedulerInbound) Leader(ctx context.Context, in *intra.LeaderRequest) (*intra.LeaderResponse, error) {
	leader, err := i.ioFetcher.Leader(in.Name)
	if err != nil {
		return nil, err
	}

	return &intra.LeaderResponse{Addr: leader}, nil
}

func (i *SchedulerInbound) Status(ctx context.Context, in *intra.StatusRequest) (*intra.StatusResponse, error) {
	status := i.ioFetcher.Status()
	resp := &intra.StatusResponse{
		ExternalAddr: i.externalAddr,
	}
	for name, peers := range status {
		resp.Buffers = append(resp.Buffers, &intra.StatusBufferInfo{
			Name:          name,
			ExpectedNodes: peers,
		})
	}

	return resp, nil
}

func (i *SchedulerInbound) UpdateConfig(ctx context.Context, in *intra.UpdateConfigRequest) (*intra.UpdateConfigResponse, error) {
	if err := i.ioFetcher.SetExpectedPeers(in.Name, in.ExpectedNodes); err != nil {
		return nil, err
	}

	return new(intra.UpdateConfigResponse), nil
}

func (i *SchedulerInbound) convertPeers(in *intra.CreateInfo) []string {
	var peers []string
	for _, peer := range in.Peers {
		peers = append(peers, peer.Addr)
	}
	return peers
}