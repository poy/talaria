package broker_test

import "github.com/apoydence/talaria/broker"

type mockController struct {
	fetchFileCh    chan string
	fetchFileIdCh  chan uint64
	fetchFileErrCh chan *broker.FetchFileError

	writeFileIdCh     chan uint64
	writeFileDataCh   chan []byte
	writeFileOffsetCh chan int64
	writeFileErrCh    chan error

	readFileIdCh   chan uint64
	readFileDataCh chan []byte
	readFileErrCh  chan error
}

func newMockController() *mockController {
	return &mockController{
		fetchFileCh:    make(chan string, 100),
		fetchFileIdCh:  make(chan uint64, 100),
		fetchFileErrCh: make(chan *broker.FetchFileError, 100),

		writeFileIdCh:     make(chan uint64, 100),
		writeFileDataCh:   make(chan []byte, 100),
		writeFileOffsetCh: make(chan int64, 100),
		writeFileErrCh:    make(chan error, 100),

		readFileIdCh:   make(chan uint64, 100),
		readFileDataCh: make(chan []byte, 100),
		readFileErrCh:  make(chan error, 100),
	}
}

func (m *mockController) FetchFile(name string) (uint64, *broker.FetchFileError) {
	m.fetchFileCh <- name
	return <-m.fetchFileIdCh, <-m.fetchFileErrCh
}

func (m *mockController) WriteToFile(id uint64, data []byte) (int64, error) {
	m.writeFileIdCh <- id
	m.writeFileDataCh <- data
	return <-m.writeFileOffsetCh, <-m.writeFileErrCh
}

func (m *mockController) ReadFromFile(id uint64) ([]byte, error) {
	m.readFileIdCh <- id
	return <-m.readFileDataCh, <-m.readFileErrCh
}
