package files

import (
	"encoding/binary"
	"io"
)

type ChunkedFileWriter struct {
	writer io.Writer
	buffer []byte
}

func NewChunkedFileWriter(writer io.Writer) *ChunkedFileWriter {
	return &ChunkedFileWriter{
		writer: writer,
		buffer: make([]byte, 8),
	}
}

func (c *ChunkedFileWriter) Write(data []byte) (int, error) {
	binary.LittleEndian.PutUint64(c.buffer, uint64(len(data)))
	fullBuffer := append(c.buffer, data...)

	n, err := c.writer.Write(fullBuffer)
	if err != nil {
		return -1, nil
	}

	if n != len(fullBuffer) {
		return -1, io.ErrShortWrite
	}

	return len(data), nil
}
