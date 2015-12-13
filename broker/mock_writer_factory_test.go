package broker_test

import "io"

type mockWriterFactory struct {
	nameCh   chan string
	resultCh chan io.Writer
}

func newMockWriterFactory() *mockWriterFactory {
	return &mockWriterFactory{
		nameCh:   make(chan string, 100),
		resultCh: make(chan io.Writer, 100),
	}
}

func (m *mockWriterFactory) ProvideWriter(name string) io.Writer {
	m.nameCh <- name
	return <-m.resultCh
}
