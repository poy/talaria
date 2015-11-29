package files_test

type mockWriter struct {
	dataChan   chan []byte
	indexCh    chan int64
	retIndexCh chan int64
}

func newMockWriter() *mockWriter {
	return &mockWriter{
		dataChan:   make(chan []byte, 100),
		indexCh:    make(chan int64, 100),
		retIndexCh: make(chan int64, 100),
	}
}

func (m *mockWriter) Write(data []byte) (int, error) {
	m.dataChan <- data
	return len(data), nil
}

func (m *mockWriter) InitWriteIndex(index int64, data []byte) (int64, error) {
	m.dataChan <- data
	m.indexCh <- index
	return <-m.retIndexCh, nil
}
