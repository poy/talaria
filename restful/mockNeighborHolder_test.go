package restful_test

import (
	"github.com/apoydence/talaria/neighbors"
)

type mockNeighborHolder struct {
	neighbors     []neighbors.Neighbor
	lastBlacklist []string
}

func NewMockNeighborHolder(ns ...string) *mockNeighborHolder {
	result := make([]neighbors.Neighbor, 0)
	for _, n := range ns {
		result = append(result, neighbors.NewNeighbor(n))
	}
	return &mockNeighborHolder{
		neighbors: result,
	}
}

func (m *mockNeighborHolder) GetNeighbors(blacklist ...string) []neighbors.Neighbor {
	m.lastBlacklist = blacklist
	return m.neighbors
}
