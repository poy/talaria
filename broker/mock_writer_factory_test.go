package broker_test

import "github.com/apoydence/talaria/broker"

type mockWriterFactory struct {
	nameCh   chan string
	resultCh chan broker.SubscribableWriter
}

func newMockWriterFactory() *mockWriterFactory {
	return &mockWriterFactory{
		nameCh:   make(chan string, 100),
		resultCh: make(chan broker.SubscribableWriter, 100),
	}
}

func (m *mockWriterFactory) ProvideWriter(name string) broker.SubscribableWriter {
	m.nameCh <- name
	return <-m.resultCh
}
