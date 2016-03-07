package client_test

import "github.com/apoydence/talaria/common"

type mockConnection struct {
	fileIdCh chan uint64
	dataCh   chan []byte
	indexCh  chan int64
}

func newMockConnection() *mockConnection {
	return &mockConnection{
		fileIdCh: make(chan uint64, 100),
		dataCh:   make(chan []byte, 100),
		indexCh:  make(chan int64, 100),
	}
}

func (m *mockConnection) WriteToFile(fileId uint64, data []byte) (int64, *common.ConnectionError) {
	m.fileIdCh <- fileId
	m.dataCh <- data
	return 99, nil
}

func (m *mockConnection) InitWriteIndex(fileId uint64, index int64, data []byte) (int64, *common.ConnectionError) {
	m.indexCh <- index
	m.dataCh <- data
	m.fileIdCh <- fileId
	return index, nil
}
