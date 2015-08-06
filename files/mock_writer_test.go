package files_test

type mockWriter struct {
	dataChan chan []byte
}

func newMockWriter() *mockWriter {
	return &mockWriter{
		dataChan: make(chan []byte, 100),
	}
}

func (m *mockWriter) Write(data []byte) (int, error) {
	m.dataChan <- data
	return len(data), nil
}
