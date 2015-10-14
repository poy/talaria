package broker_test

type mockWriter struct {
	dataCh chan []byte
}

func newMockWriter() *mockWriter {
	return &mockWriter{
		dataCh: make(chan []byte, 100),
	}
}

func (m *mockWriter) Write(data []byte) (int, error) {
	m.dataCh <- data
	return len(data), nil
}
