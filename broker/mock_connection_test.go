package broker_test

type mockConnection struct {
	fileIdCh chan uint64
	dataCh   chan []byte
}

func newMockConnection() *mockConnection {
	return &mockConnection{
		fileIdCh: make(chan uint64, 100),
		dataCh:   make(chan []byte, 100),
	}
}

func (m *mockConnection) WriteToFile(fileId uint64, data []byte) (int64, error) {
	m.fileIdCh <- fileId
	m.dataCh <- data
	return 99, nil
}
