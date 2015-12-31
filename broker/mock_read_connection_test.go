package broker_test

import "github.com/apoydence/talaria/broker"

type mockReadConnection struct {
	fileIdCh    chan uint64
	resultCh    chan []byte
	indexCh     chan int64
	seekIndexCh chan uint64
	errCh       chan *broker.ConnectionError
	seekErrCh   chan *broker.ConnectionError
}

func newMockReadConnection() *mockReadConnection {
	return &mockReadConnection{
		fileIdCh:    make(chan uint64, 100),
		resultCh:    make(chan []byte, 100),
		indexCh:     make(chan int64, 100),
		seekIndexCh: make(chan uint64, 100),
		errCh:       make(chan *broker.ConnectionError, 100),
		seekErrCh:   make(chan *broker.ConnectionError, 100),
	}
}

func (m *mockReadConnection) ReadFromFile(fileId uint64) ([]byte, int64, *broker.ConnectionError) {
	m.fileIdCh <- fileId
	return <-m.resultCh, <-m.indexCh, <-m.errCh
}

func (m *mockReadConnection) SeekIndex(fileId, index uint64) *broker.ConnectionError {
	m.fileIdCh <- fileId
	m.seekIndexCh <- index
	return <-m.seekErrCh
}
