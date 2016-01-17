package leadervalidator_test

type mockReplicaFetcher struct {
	nameCh    chan string
	replicaCh chan []string
}

func newMockReplicaFetcher() *mockReplicaFetcher {
	return &mockReplicaFetcher{
		nameCh:    make(chan string, 100),
		replicaCh: make(chan []string, 100),
	}
}

func (m *mockReplicaFetcher) FetchReplicas(name string) []string {
	m.nameCh <- name
	return <-m.replicaCh
}
