package orchestrator_test

import "io"

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

func (m *mockPartitionManager) ProvideWriter(name string, index uint) io.Writer {
	m.addCh <- name
	m.indexCh <- index
	return nil
}

func (m *mockPartitionManager) Participate(name string, index uint) bool {
	m.addCh <- name
	m.indexCh <- index
	return <-m.partCh
}
