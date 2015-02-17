package neighbors

type NeighborCollection struct {
	ns []Neighbor
}

func NewNeighborCollection(ns ...Neighbor) *NeighborCollection {
	return &NeighborCollection{
		ns: ns,
	}
}

func (nc *NeighborCollection) GetNeighbors(blacklist ...string) []Neighbor {
	results := make([]Neighbor, 0)
	for _, n := range nc.ns {
		if !blacklistContains(blacklist, n.Endpoint) {
			results = append(results, n)
		}
	}
	return results
}

func blacklistContains(blacklist []string, endpoint string) bool {
	for _, s := range blacklist {
		if s == endpoint {
			return true
		}
	}
	return false
}
