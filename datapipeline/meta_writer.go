package datapipeline

import (
	"encoding/binary"
	"io"
)

type MetaWriter struct {
	lengthBuffer []byte
	writer       io.Writer
}

func NewMetaWriter(writer io.Writer) *MetaWriter {
	return &MetaWriter{
		writer:       writer,
		lengthBuffer: make([]byte, 4),
	}
}

func (p *MetaWriter) Write(data []byte) (int, error) {
	binary.LittleEndian.PutUint32(p.lengthBuffer, uint32(len(data)))
	return p.writer.Write(append(p.lengthBuffer, data...))
}
