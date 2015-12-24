package broker

import (
	"fmt"
	"io"

	"github.com/apoydence/talaria/logging"
)

type OffsetReader interface {
	io.Reader
	NextIndex() int64
	SeekIndex(index uint64) error
}

type InitableWriter interface {
	io.Writer
	InitWriteIndex(index int64, data []byte) (int64, error)
}

type IoProvider interface {
	ProvideWriter(name string) InitableWriter
	ProvideReader(name string) OffsetReader
}

type Orchestrator interface {
	FetchLeader(name string) (string, bool)
}

type ioInfo struct {
	name         string
	writer       io.Writer
	reader       OffsetReader
	writerOffset int64
	buffer       []byte
}

type FileController struct {
	log          logging.Logger
	fileIdMap    map[uint64]*ioInfo
	skipOrch     bool
	ioProvider   IoProvider
	orchestrator Orchestrator
}

func NewFileController(skipOrch bool, ioProvider IoProvider, orchestrator Orchestrator) *FileController {
	return &FileController{
		log:          logging.Log("FileController"),
		fileIdMap:    make(map[uint64]*ioInfo),
		skipOrch:     skipOrch,
		ioProvider:   ioProvider,
		orchestrator: orchestrator,
	}
}

func (f *FileController) FetchFile(fileId uint64, name string) *ConnectionError {
	info, ok := f.fileIdMap[fileId]
	if ok && name != info.name {
		return NewConnectionError(fmt.Sprintf("ID (%d) already used with %s", fileId, info.name), "", false)
	}

	if !ok {

		f.log.Debug("New fileId (fileId=%d) (skipOrch=%v) %s", fileId, f.skipOrch, name)

		if !f.skipOrch {
			uri, local := f.orchestrator.FetchLeader(name)
			if !local {
				return NewConnectionError("Redirect to the correct broker", uri, false)
			}
		}

		f.fileIdMap[fileId] = &ioInfo{
			name:   name,
			writer: f.ioProvider.ProvideWriter(name),
			reader: f.ioProvider.ProvideReader(name),
			buffer: make([]byte, 1024),
		}
	}
	return nil
}

func (f *FileController) WriteToFile(fileId uint64, data []byte) (int64, error) {
	ioInfo, ok := f.fileIdMap[fileId]
	if !ok {
		return 0, fmt.Errorf("Unknown file ID: %d", fileId)
	}

	n, err := ioInfo.writer.Write(data)
	ioInfo.writerOffset += int64(n)
	return ioInfo.writerOffset, err
}

func (f *FileController) ReadFromFile(fileId uint64, callback func([]byte, int64, error)) {
	ioInfo, ok := f.fileIdMap[fileId]
	if !ok {
		callback(nil, 0, fmt.Errorf("Unknown file ID: %d", fileId))
		return
	}

	go func() {
		n, err := ioInfo.reader.Read(ioInfo.buffer)
		if err != nil {
			callback(nil, 0, err)
			return
		}

		offset := ioInfo.reader.NextIndex() - 1
		callback(ioInfo.buffer[:n], offset, nil)
	}()
}

func (f *FileController) SeekIndex(fileId, index uint64) error {
	ioInfo, ok := f.fileIdMap[fileId]
	if !ok {
		return fmt.Errorf("Unknown file ID: %d", fileId)
	}

	return ioInfo.reader.SeekIndex(index)
}
