package neighbors

type Neighbor struct {
	Endpoint string
}

func NewNeighbor(endpoint string) Neighbor {
	return Neighbor{
		Endpoint: endpoint,
	}
}
