package client_test

import "github.com/apoydence/talaria/client"

type mockInnerBrokerProvider struct {
	nameCh   chan string
	addrCh   chan string
	resultCh chan *client.Reader
}

func newMockInnerBrokerProvider() *mockInnerBrokerProvider {
	return &mockInnerBrokerProvider{
		nameCh:   make(chan string, 100),
		addrCh:   make(chan string, 100),
		resultCh: make(chan *client.Reader, 100),
	}
}

func (m *mockInnerBrokerProvider) ProvideConn(name, addr string) *client.Reader {
	m.nameCh <- name
	m.addrCh <- addr
	return <-m.resultCh
}
