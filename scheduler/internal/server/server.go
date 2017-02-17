package server

import (
	"fmt"
	"log"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/api/intra"
	pb "github.com/apoydence/talaria/api/v1"
)

type NodeInfo struct {
	Client intra.NodeClient
	URI    string
}

type Auditor interface {
	List() pb.ListResponse
}

type NodeFetcher interface {
	FetchNodes(count int, exclude ...NodeInfo) (client []NodeInfo)
	FetchNode(addr string) (node NodeInfo, err error)
}

type Server struct {
	fetcher NodeFetcher
	auditor Auditor
}

func New(fetcher NodeFetcher, auditor Auditor) *Server {
	return &Server{
		fetcher: fetcher,
		auditor: auditor,
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
		Name:       info.Name,
		Peers:      s.buildPeerList(nodes),
		BufferSize: info.BufferSize,
	}

	if intraInfo.BufferSize == 0 {
		intraInfo.BufferSize = 100
	}

	for _, node := range nodes {
		log.Printf("Creating buffer %s on node %s", info.Name, node.URI)
		if _, err := node.Client.Create(ctx, intraInfo); err != nil {
			log.Printf("Error scheduling %s on %s: %s", info.Name, node.URI, err)
			continue
		}

		successfulNodes = append(successfulNodes, node)
	}

	if len(successfulNodes) == 0 {
		return nil, fmt.Errorf("failed to create buffer")
	}

	return new(pb.CreateResponse), nil
}

func (s *Server) ListClusterInfo(ctx context.Context, info *pb.ListInfo) (*pb.ListResponse, error) {
	list := s.auditor.List()
	if len(info.Names) == 0 {
		return &list, nil
	}

	var results []*pb.ClusterInfo
	for _, c := range list.Info {
		if s.contains(c.Name, info.Names) {
			results = append(results, c)
		}
	}

	return &pb.ListResponse{
		Info: results,
	}, nil
}

func (s *Server) contains(str string, values []string) bool {
	for _, v := range values {
		if v == str {
			return true
		}
	}
	return false
}

func (s *Server) buildPeerList(nodes []NodeInfo) []*intra.PeerInfo {
	var peers []*intra.PeerInfo
	for _, node := range nodes {
		peers = append(peers, &intra.PeerInfo{Addr: node.URI})
	}

	return peers
}
