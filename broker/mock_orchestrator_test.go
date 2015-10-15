package broker_test

type mockOrchestrator struct {
	nameCh        chan string
	leadersOnlyCh chan bool
	uriCh         chan string
	localCh       chan bool
}

func newMockOrchestrator() *mockOrchestrator {
	return &mockOrchestrator{
		nameCh:  make(chan string, 100),
		uriCh:   make(chan string, 100),
		localCh: make(chan bool, 100),
	}
}

func (m *mockOrchestrator) FetchLeader(name string) (string, bool) {
	m.nameCh <- name
	return <-m.uriCh, <-m.localCh
}
