package broker_test

type mockOrchestrator struct {
	nameCh   chan string
	createCh chan bool
	uriCh    chan string
	localCh  chan bool
	errCh    chan error
}

func newMockOrchestrator() *mockOrchestrator {
	return &mockOrchestrator{
		nameCh:   make(chan string, 100),
		uriCh:    make(chan string, 100),
		localCh:  make(chan bool, 100),
		createCh: make(chan bool, 100),
		errCh:    make(chan error, 100),
	}
}

func (m *mockOrchestrator) FetchLeader(name string, create bool) (string, bool, error) {
	m.nameCh <- name
	m.createCh <- create
	return <-m.uriCh, <-m.localCh, <-m.errCh
}
