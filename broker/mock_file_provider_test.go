package broker_test

import (
	"io"

	"github.com/apoydence/talaria/broker"
)

type mockFileProvider struct {
	writerNameCh      chan string
	readerNameCh      chan string
	writerCh          chan io.Writer
	readerCh          chan *mockReader
	indexCh           chan int64
	initIndexResultCh chan int64
}

type subWrapper struct {
	io.Writer
	indexCh           chan int64
	initIndexResultCh chan int64
}

type indexWrapper struct {
	reader  io.Reader
	indexCh chan int64
}

func newMockFileProvider() *mockFileProvider {
	return &mockFileProvider{
		writerNameCh:      make(chan string, 100),
		readerNameCh:      make(chan string, 100),
		writerCh:          make(chan io.Writer, 100),
		readerCh:          make(chan *mockReader, 100),
		indexCh:           make(chan int64, 100),
		initIndexResultCh: make(chan int64, 100),
	}
}

func (m *mockFileProvider) ProvideWriter(name string) broker.InitableWriter {
	m.writerNameCh <- name
	return newSubWrapper(<-m.writerCh, m.indexCh, m.initIndexResultCh)
}

func (m *mockFileProvider) ProvideReader(name string) broker.IndexReader {
	m.readerNameCh <- name
	return <-m.readerCh
}

func newSubWrapper(writer io.Writer, indexCh, initIndexResultCh chan int64) *subWrapper {
	return &subWrapper{
		Writer:            writer,
		indexCh:           indexCh,
		initIndexResultCh: initIndexResultCh,
	}
}

func (s *subWrapper) InitWriteIndex(index int64, data []byte) (int64, error) {
	s.Write(data)
	s.indexCh <- index
	return <-s.initIndexResultCh, nil
}
