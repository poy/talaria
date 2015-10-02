package broker_test

import "io"

type mockFileProvider struct {
	writerNameCh chan string
	replicaCh    chan uint
	readerNameCh chan string
	writerCh     chan io.Writer
	readerCh     chan io.Reader
}

func newMockFileProvider() *mockFileProvider {
	return &mockFileProvider{
		writerNameCh: make(chan string, 100),
		replicaCh:    make(chan uint, 100),
		readerNameCh: make(chan string, 100),
		writerCh:     make(chan io.Writer, 100),
		readerCh:     make(chan io.Reader, 100),
	}
}

func (m *mockFileProvider) ProvideWriter(name string, replica uint) io.Writer {
	m.writerNameCh <- name
	m.replicaCh <- replica
	return <-m.writerCh
}

func (m *mockFileProvider) ProvideReader(name string) io.Reader {
	m.readerNameCh <- name
	return <-m.readerCh
}
