package datapipeline_test

import "fmt"

type mockFile struct {
	buffer    []byte
	index     int64
	fileNum   int
	closeChan chan int
}

func newMockFile(buffer []byte, fileNum int, closeChan chan int) *mockFile {
	return &mockFile{
		buffer:    buffer,
		closeChan: closeChan,
		fileNum:   fileNum,
	}
}

func (m *mockFile) Write(data []byte) (int, error) {
	if len(data) > len(m.buffer)-int(m.index) {
		panic("data is too long")
	}

	for _, d := range data {
		m.buffer[m.index] = d
		m.index++
	}

	return len(data), nil
}

func (m *mockFile) Seek(offset int64, whence int) (int64, error) {
	if offset < 0 {
		return -1, fmt.Errorf("offset can't be negative")
	}

	switch whence {
	case 0:
		m.index = offset
	case 1:
		m.index += offset
	case 2:
		m.index = int64(len(m.buffer)) - 1 - offset
	default:
		return -1, fmt.Errorf("whence (%d) has to be 0, 1, or 2", whence)
	}

	return m.index, nil
}

func (m *mockFile) Close() error {
	m.closeChan <- m.fileNum
	return nil
}
