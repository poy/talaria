package broker_test

import "io"

type mockInnerBrokerProvider struct {
	nameCh   chan string
	addrCh   chan string
	resultCh chan io.Writer
}

func newMockInnerBrokerProvider() *mockInnerBrokerProvider {
	return &mockInnerBrokerProvider{
		nameCh:   make(chan string, 100),
		addrCh:   make(chan string, 100),
		resultCh: make(chan io.Writer, 100),
	}
}

func (m *mockInnerBrokerProvider) ProvideConn(name, addr string) io.Writer {
	m.nameCh <- name
	m.addrCh <- addr
	return <-m.resultCh
}
