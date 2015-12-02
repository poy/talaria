package broker

import (
	"fmt"
	"io"

	"github.com/apoydence/talaria/logging"
)

type OffsetReader interface {
	io.Reader
	NextIndex() int64
}

type IoProvider interface {
	ProvideWriter(name string) SubscribableWriter
	ProvideReader(name string) OffsetReader
}

type Orchestrator interface {
	FetchLeader(name string) (string, bool)
}

type ioInfo struct {
	name         string
	writer       SubscribableWriter
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

func (f *FileController) FetchFile(fileId uint64, name string) *FetchFileError {
	info, ok := f.fileIdMap[fileId]
	if ok && name != info.name {
		return NewFetchFileError(fmt.Sprintf("ID (%d) already used with %s", fileId, info.name), "")
	}

	if !ok {

		f.log.Debug("New fileId (fileId=%d) (skipOrch=%v) %s", fileId, f.skipOrch, name)

		if !f.skipOrch {
			uri, local := f.orchestrator.FetchLeader(name)
			if !local {
				return NewFetchFileError("Redirect to the correct broker", uri)
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

func (f *FileController) ReadFromFile(fileId uint64) ([]byte, int64, error) {
	ioInfo, ok := f.fileIdMap[fileId]
	if !ok {
		return nil, 0, fmt.Errorf("Unknown file ID: %d", fileId)
	}

	n, err := ioInfo.reader.Read(ioInfo.buffer)
	if err != nil {
		return nil, 0, err
	}

	offset := ioInfo.reader.NextIndex() - 1
	return ioInfo.buffer[:n], offset, nil
}

func (f *FileController) InitWriteIndex(fileId uint64, index int64, data []byte) (int64, error) {
	ioInfo, ok := f.fileIdMap[fileId]
	if !ok {
		return 0, fmt.Errorf("Unknown file ID: %d", fileId)
	}

	return ioInfo.writer.InitWriteIndex(index, data)
}
