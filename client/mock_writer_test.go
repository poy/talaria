package client_test

type mockWriter struct {
	indexCh chan int64
	dataCh  chan []byte
}

func newMockWriter() *mockWriter {
	return &mockWriter{
		indexCh: make(chan int64, 100),
		dataCh:  make(chan []byte, 100),
	}
}

func (m *mockWriter) Write(data []byte) (int, error) {
	m.dataCh <- data
	return len(data), nil
}

func (m *mockWriter) InitWriteIndex(index int64, data []byte) (int64, error) {
	m.indexCh <- index
	m.dataCh <- data
	return index, nil
}
