package orchestrator_test

type mockIndexProvider struct {
	nameCh  chan string
	indexCh chan uint64
	okCh    chan bool
}

func newMockIndexProvider() *mockIndexProvider {
	return &mockIndexProvider{
		nameCh:  make(chan string, 100),
		indexCh: make(chan uint64, 100),
		okCh:    make(chan bool, 100),
	}
}

func (m *mockIndexProvider) ProvideLastIndex(name string) (uint64, bool) {
	m.nameCh <- name
	return <-m.indexCh, <-m.okCh
}
