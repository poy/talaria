package broker_test

import "io"

type mockInnerBrokerProvider struct {
	addrCh   chan string
	resultCh chan io.Writer
}

func newMockInnerBrokerProvider() *mockInnerBrokerProvider {
	return &mockInnerBrokerProvider{
		addrCh:   make(chan string, 100),
		resultCh: make(chan io.Writer, 100),
	}
}

func (m *mockInnerBrokerProvider) ProvideConn(addr string) io.Writer {
	m.addrCh <- addr
	return <-m.resultCh
}
