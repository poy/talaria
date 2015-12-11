package broker_test

import "github.com/apoydence/talaria/broker"

type mockDirectConnection struct {
	fileIdCh chan uint64
	resultCh chan []byte
	indexCh  chan int64
	errCh    chan *broker.ConnectionError
}

func newMockDirectConnection() *mockDirectConnection {
	return &mockDirectConnection{
		fileIdCh: make(chan uint64, 100),
		resultCh: make(chan []byte, 100),
		indexCh:  make(chan int64, 100),
		errCh:    make(chan *broker.ConnectionError, 100),
	}
}

func (m *mockDirectConnection) ReadFromFile(fileId uint64) ([]byte, int64, *broker.ConnectionError) {
	m.fileIdCh <- fileId
	return <-m.resultCh, <-m.indexCh, <-m.errCh
}
