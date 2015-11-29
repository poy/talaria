package broker_test

import "github.com/apoydence/talaria/files"

type mockInnerBrokerProvider struct {
	nameCh   chan string
	addrCh   chan string
	resultCh chan files.InitableWriter
}

func newMockInnerBrokerProvider() *mockInnerBrokerProvider {
	return &mockInnerBrokerProvider{
		nameCh:   make(chan string, 100),
		addrCh:   make(chan string, 100),
		resultCh: make(chan files.InitableWriter, 100),
	}
}

func (m *mockInnerBrokerProvider) ProvideConn(name, addr string) files.InitableWriter {
	m.nameCh <- name
	m.addrCh <- addr
	return <-m.resultCh
}
