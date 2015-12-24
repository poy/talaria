package broker_test

type mockReader struct {
	buffers     chan []byte
	lengths     chan int
	nextIndexes chan int64
	readErrs    chan error
	seekIndexes chan uint64
	seekErrs    chan error
}

func newMockReader() *mockReader {
	return &mockReader{
		buffers:     make(chan []byte, 100),
		lengths:     make(chan int, 100),
		nextIndexes: make(chan int64, 100),
		readErrs:    make(chan error, 100),
		seekIndexes: make(chan uint64, 100),
		seekErrs:    make(chan error, 100),
	}
}

func (m *mockReader) Read(buffer []byte) (int, error) {
	m.buffers <- buffer
	return <-m.lengths, <-m.readErrs
}

func (m *mockReader) NextIndex() int64 {
	return <-m.nextIndexes
}

func (m *mockReader) SeekIndex(index uint64) error {
	m.seekIndexes <- index
	return <-m.seekErrs
}
