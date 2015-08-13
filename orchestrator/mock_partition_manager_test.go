package orchestrator_test

type mockPartitionManager struct {
	addCh chan string
}

func newMockPartitionManager() *mockPartitionManager {
	return &mockPartitionManager{
		addCh: make(chan string, 100),
	}
}

func (m *mockPartitionManager) Add(name string) {
	m.addCh <- name
}
