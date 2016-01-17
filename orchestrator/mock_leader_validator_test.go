package orchestrator_test

type mockLeaderValidator struct {
	nameCh     chan string
	indexCh    chan uint64
	callbackCh chan func(string, bool)
}

func newMockLeaderValidator() *mockLeaderValidator {
	return &mockLeaderValidator{
		nameCh:     make(chan string, 100),
		indexCh:    make(chan uint64, 100),
		callbackCh: make(chan func(string, bool), 100),
	}
}

func (m *mockLeaderValidator) Validate(name string, index uint64, callback func(string, bool)) {
	m.nameCh <- name
	m.indexCh <- index
	m.callbackCh <- callback
}
