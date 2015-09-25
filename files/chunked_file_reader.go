package files

import (
	"encoding/binary"
	"fmt"
	"io"
)

type ChunkedFileReader struct {
	reader       io.Reader
	lengthBuffer []byte
}

func NewChunkedFileReader(reader io.Reader) *ChunkedFileReader {
	return &ChunkedFileReader{
		reader:       reader,
		lengthBuffer: make([]byte, 8),
	}
}

func (c *ChunkedFileReader) Read(buffer []byte) (int, error) {
	n, err := c.reader.Read(c.lengthBuffer)
	if err != nil {
		return -1, err
	}

	if n != 8 {
		return -1, io.ErrUnexpectedEOF
	}

	length := int(binary.LittleEndian.Uint64(c.lengthBuffer))
	if length > len(buffer) || length < 0 {
		return -1, fmt.Errorf("invalid length: %d", length)
	}

	n, err = c.reader.Read(buffer[:length])
	if err != nil {
		return -1, err
	}

	if n != length {
		return -1, io.ErrUnexpectedEOF
	}

	return length, nil
}
