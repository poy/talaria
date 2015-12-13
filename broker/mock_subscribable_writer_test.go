package broker_test

import "io"

type mockSubscribableWriter struct {
	subWriterCh chan io.Writer
	dataCh      chan []byte
}

func newMockSubscribableWriter() *mockSubscribableWriter {
	return &mockSubscribableWriter{
		subWriterCh: make(chan io.Writer, 100),
		dataCh:      make(chan []byte, 100),
	}
}

func (m *mockSubscribableWriter) UpdateWriter(writer io.Writer) {
	m.subWriterCh <- writer
}

func (m *mockSubscribableWriter) InitWriteIndex(index int64, data []byte) (int64, error) {
	//NOP
	return 0, nil
}

func (m *mockSubscribableWriter) Write(data []byte) (int, error) {
	m.dataCh <- data
	return len(data), nil
}
