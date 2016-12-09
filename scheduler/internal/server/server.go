package server

import (
	"fmt"
	"log"
	"math/rand"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
)

type NodeInfo struct {
	Client intra.NodeClient
	URI    string
	ID     uint64
}

type NodeFetcher interface {
	FetchNodes(count int, exclude ...NodeInfo) (client []NodeInfo)
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
	nodes := s.fetcher.FetchNodes(3)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("No nodes available")
	}

	var successfulNodes []NodeInfo

	intraInfo := &intra.CreateInfo{
		Name:  info.Name,
		Peers: s.buildPeerList(nodes),
	}

	for _, node := range nodes {
		log.Printf("Creating buffer %s on node %s (ID=%d)", info.Name, node.URI, node.ID)
		if _, err := node.Client.Create(ctx, intraInfo); err != nil {
			log.Printf("Error scheduling %s on %s: %s", info.Name, node.URI, err)
			continue
		}

		successfulNodes = append(successfulNodes, node)
	}

	if len(successfulNodes) == 0 {
		return nil, fmt.Errorf("failed to create buffer")
	}

	seed := rand.Intn(1000)
	for i := range nodes {
		log.Printf("Fetching leader for %s", info.Name)
		leaderInfo, err := successfulNodes[(i+seed)%len(successfulNodes)].Client.Leader(ctx, &intra.LeaderRequest{
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

func (s *Server) buildPeerList(nodes []NodeInfo) []*intra.PeerInfo {
	var peers []*intra.PeerInfo
	for _, node := range nodes {
		peers = append(peers, &intra.PeerInfo{Id: node.ID})
	}

	return peers
}

func (s *Server) findViaID(ID uint64, nodes []NodeInfo) (NodeInfo, bool) {
	for _, n := range nodes {
		if ID == n.ID {
			return n, true
		}
	}

	return NodeInfo{}, false
}
