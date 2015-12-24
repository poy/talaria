package broker_test

import "github.com/apoydence/talaria/broker"

type mockInnerBrokerProvider struct {
	nameCh   chan string
	addrCh   chan string
	resultCh chan *broker.Reader
}

func newMockInnerBrokerProvider() *mockInnerBrokerProvider {
	return &mockInnerBrokerProvider{
		nameCh:   make(chan string, 100),
		addrCh:   make(chan string, 100),
		resultCh: make(chan *broker.Reader, 100),
	}
}

func (m *mockInnerBrokerProvider) ProvideConn(name, addr string) *broker.Reader {
	m.nameCh <- name
	m.addrCh <- addr
	return <-m.resultCh
}
