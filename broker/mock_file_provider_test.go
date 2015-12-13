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
	indexCh      chan int64
}

type subWrapper struct {
	io.Writer
	indexCh chan int64
}

type offsetWrapper struct {
	reader  io.Reader
	indexCh chan int64
}

func newMockFileProvider() *mockFileProvider {
	return &mockFileProvider{
		writerNameCh: make(chan string, 100),
		readerNameCh: make(chan string, 100),
		writerCh:     make(chan io.Writer, 100),
		readerCh:     make(chan io.Reader, 100),
		indexCh:      make(chan int64, 100),
	}
}

func (m *mockFileProvider) ProvideWriter(name string) io.Writer {
	m.writerNameCh <- name
	return newSubWrapper(<-m.writerCh, m.indexCh)
}

func (m *mockFileProvider) ProvideReader(name string) broker.OffsetReader {
	m.readerNameCh <- name
	return newIndexWrapper(<-m.readerCh, m.indexCh)
}

func newSubWrapper(writer io.Writer, indexCh chan int64) *subWrapper {
	return &subWrapper{
		Writer:  writer,
		indexCh: indexCh,
	}
}

func newIndexWrapper(reader io.Reader, indexCh chan int64) *offsetWrapper {
	return &offsetWrapper{
		reader:  reader,
		indexCh: indexCh,
	}
}

func (o *offsetWrapper) Read(buffer []byte) (int, error) {
	n, err := o.reader.Read(buffer)
	return n, err
}

func (o *offsetWrapper) NextIndex() int64 {
	return <-o.indexCh
}
