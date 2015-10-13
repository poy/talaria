package broker_test

import (
	"io"

	"github.com/apoydence/talaria/broker"
)

type mockFileProvider struct {
	writerNameCh chan string
	readerNameCh chan string
	writerCh     chan io.Writer
	readerCh     chan io.Reader
}

type subWrapper struct {
	io.Writer
}

func newMockFileProvider() *mockFileProvider {
	return &mockFileProvider{
		writerNameCh: make(chan string, 100),
		readerNameCh: make(chan string, 100),
		writerCh:     make(chan io.Writer, 100),
		readerCh:     make(chan io.Reader, 100),
	}
}

func (m *mockFileProvider) ProvideWriter(name string) broker.SubscribableWriter {
	m.writerNameCh <- name
	return newSubWrapper(<-m.writerCh)
}

func (m *mockFileProvider) ProvideReader(name string) io.Reader {
	m.readerNameCh <- name
	return <-m.readerCh
}

func newSubWrapper(writer io.Writer) *subWrapper {
	return &subWrapper{
		Writer: writer,
	}
}

func (s *subWrapper) UpdateWriter(io.Writer) {
	// NOP
}
