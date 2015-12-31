package broker_test

import "github.com/apoydence/talaria/broker"

type mockWriteConnection struct {
	fileIdCh chan uint64
	dataCh   chan []byte
	indexCh  chan int64
	errCh    chan *broker.ConnectionError
}

func newMockWriteConnection() *mockWriteConnection {
	return &mockWriteConnection{
		fileIdCh: make(chan uint64, 100),
		dataCh:   make(chan []byte, 100),
		indexCh:  make(chan int64, 100),
		errCh:    make(chan *broker.ConnectionError, 100),
	}
}

func (m *mockWriteConnection) WriteToFile(fileId uint64, data []byte) (int64, *broker.ConnectionError) {
	m.fileIdCh <- fileId
	m.dataCh <- data
	return <-m.indexCh, <-m.errCh
}
