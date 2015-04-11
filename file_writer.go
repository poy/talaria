package talaria

import (
	"encoding/binary"
	"io"
)

type WriteSeekCloser interface {
	io.WriteSeeker
	io.Closer
}

type FileGenerator interface {
	CreateFile() WriteSeekCloser
}

type FileWriter struct {
	fileGenerator FileGenerator
	limitLength   uint32
	currentLength uint32
	count         uint32
	lengthBuffer  []byte
	file          WriteSeekCloser
}

func NewFileWriter(fileGenerator FileGenerator, limit uint32) *FileWriter {
	f := &FileWriter{
		fileGenerator: fileGenerator,
		limitLength:   limit,
		lengthBuffer:  make([]byte, 4),
	}
	f.initFile()
	return f
}

func (f *FileWriter) Write(data []byte) (int, error) {
	dataLen := uint32(len(data))
	for i := uint32(0); i < dataLen; i += f.limitLength {
		upper := i + f.limitLength
		upper = min(upper, dataLen)
		f.actualWrite(data[i:upper])
	}
	return int(dataLen), nil
}

func (f *FileWriter) actualWrite(data []byte) (int, error) {
	dataLen := uint32(len(data))
	if dataLen+f.currentLength >= f.limitLength {
		f.file.Seek(0, 0)
		binary.LittleEndian.PutUint32(f.lengthBuffer, f.count)
		if _, err := f.file.Write(f.lengthBuffer); err != nil {
			return -1, err
		}
		if err := f.file.Close(); err != nil {
			return -1, err
		}
		f.initFile()
	}

	f.count++
	f.currentLength += dataLen
	return f.file.Write(data)
}

func (f *FileWriter) initFile() {
	f.file = f.fileGenerator.CreateFile()
	f.file.Seek(4, 0)
	f.currentLength = 0
}

func min(a, b uint32) uint32 {
	if a > b {
		return b
	}
	return a
}
