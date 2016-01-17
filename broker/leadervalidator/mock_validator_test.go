package leadervalidator_test

type mockValidator struct {
	nameCh  chan string
	indexCh chan uint64
	retCh   chan bool
}

func newMockValidator() *mockValidator {
	return &mockValidator{
		nameCh:  make(chan string, 100),
		indexCh: make(chan uint64, 100),
		retCh:   make(chan bool, 100),
	}
}

func (m *mockValidator) ValidateLeader(name string, index uint64) bool {
	m.nameCh <- name
	m.indexCh <- index
	return <-m.retCh
}
