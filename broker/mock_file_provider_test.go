package broker_test

import "io"

type mockFileProvider struct {
	writerNameCh chan string
	readerNameCh chan string
	writerCh     chan io.Writer
	readerCh     chan io.ReadSeeker
}

func newMockFileProvider() *mockFileProvider {
	return &mockFileProvider{
		writerNameCh: make(chan string, 100),
		readerNameCh: make(chan string, 100),
		writerCh:     make(chan io.Writer, 100),
		readerCh:     make(chan io.ReadSeeker, 100),
	}
}

func (m *mockFileProvider) ProvideWriter(name string) io.Writer {
	m.writerNameCh <- name
	return <-m.writerCh
}

func (m *mockFileProvider) ProvideReader(name string) io.ReadSeeker {
	m.readerNameCh <- name
	return <-m.readerCh
}
