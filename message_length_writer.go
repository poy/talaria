package talaria

import (
	"encoding/binary"
	"io"
)

type MessageLengthWriter struct {
	lengthBuffer []byte
	writer       io.Writer
}

func NewMessageLengthWriter(writer io.Writer) *MessageLengthWriter {
	return &MessageLengthWriter{
		writer:       writer,
		lengthBuffer: make([]byte, 4),
	}
}

func (p *MessageLengthWriter) Write(data []byte) (int, error) {
	binary.LittleEndian.PutUint32(p.lengthBuffer, uint32(len(data)))
	return p.writer.Write(append(p.lengthBuffer, data...))
}
