package broker_test

type mockReader struct {
	buffers chan []byte
	lengths chan int
	errs    chan error
}

func newMockReader() *mockReader {
	return &mockReader{
		buffers: make(chan []byte, 100),
		lengths: make(chan int, 100),
		errs:    make(chan error, 100),
	}
}

func (m *mockReader) Read(buffer []byte) (int, error) {
	m.buffers <- buffer
	return <-m.lengths, <-m.errs
}
