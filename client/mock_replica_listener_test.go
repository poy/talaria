package client_test

type mockReplicaListener struct {
	nameCh     chan string
	callbackCh chan func(name string, replica uint, addr string)
}

func newMockReplicaListener() *mockReplicaListener {
	return &mockReplicaListener{
		nameCh:     make(chan string, 100),
		callbackCh: make(chan func(name string, replica uint, addr string), 100),
	}
}

func (m *mockReplicaListener) ListenForReplicas(name string, callback func(name string, replica uint, addr string)) {
	m.nameCh <- name
	m.callbackCh <- callback
}
