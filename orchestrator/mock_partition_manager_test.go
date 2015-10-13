package orchestrator_test

type mockPartitionManager struct {
	addCh   chan string
	indexCh chan uint
	partCh  chan bool
}

func newMockPartitionManager() *mockPartitionManager {
	return &mockPartitionManager{
		addCh:   make(chan string, 100),
		indexCh: make(chan uint, 100),
		partCh:  make(chan bool, 100),
	}
}

func (m *mockPartitionManager) Add(name string, index uint) {
	m.addCh <- name
	m.indexCh <- index
}

func (m *mockPartitionManager) Participate(name string, index uint) bool {
	m.addCh <- name
	m.indexCh <- index
	return <-m.partCh
}
