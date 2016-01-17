package broker

import (
	"fmt"
	"io"

	"github.com/apoydence/talaria/logging"
)

type IndexReader interface {
	io.Reader
	NextIndex() int64
	SeekIndex(index uint64) error
}

type InitableWriter interface {
	io.Writer
	InitWriteIndex(index int64, data []byte) (int64, error)
	LastIndex() uint64
}

type IoProvider interface {
	ProvideWriter(name string) InitableWriter
	ProvideReader(name string) IndexReader
}

type Orchestrator interface {
	FetchLeader(name string, create bool) (string, bool, error)
}

type ioInfo struct {
	name        string
	writer      io.Writer
	reader      IndexReader
	writerIndex int64
	buffer      []byte
}

type FileController struct {
	log          logging.Logger
	fileIdMap    map[uint64]*ioInfo
	ioProvider   IoProvider
	orchestrator Orchestrator
}

func NewFileController(ioProvider IoProvider, orchestrator Orchestrator) *FileController {
	return &FileController{
		log:          logging.Log("FileController"),
		fileIdMap:    make(map[uint64]*ioInfo),
		ioProvider:   ioProvider,
		orchestrator: orchestrator,
	}
}

func (f *FileController) FetchFile(fileId uint64, name string, create bool) *ConnectionError {
	info, ok := f.fileIdMap[fileId]
	if ok && name != info.name {
		return NewConnectionError(fmt.Sprintf("ID (%d) already used with %s", fileId, info.name), "", "", false)
	}

	if ok {
		return nil
	}

	f.log.Debug("New fileId (fileId=%d) (create=%v) %s", fileId, create, name)

	uri, local, err := f.orchestrator.FetchLeader(name, create)

	if err != nil {
		return NewConnectionError(err.Error(), "", "", false)
	}

	if !local {
		return NewConnectionError("Redirect to the correct broker", uri, "", false)
	}

	if !create && uri == "" {
		return NewConnectionError(fmt.Sprintf("File (%s) has not been created", name), "", "", false)
	}

	f.fileIdMap[fileId] = &ioInfo{
		name:   name,
		writer: f.ioProvider.ProvideWriter(name),
		reader: f.ioProvider.ProvideReader(name),
		buffer: make([]byte, 1024),
	}

	return nil
}

func (f *FileController) WriteToFile(fileId uint64, data []byte) (int64, error) {
	ioInfo, ok := f.fileIdMap[fileId]
	if !ok {
		return 0, fmt.Errorf("Unknown file ID: %d", fileId)
	}

	n, err := ioInfo.writer.Write(data)
	ioInfo.writerIndex += int64(n)
	return ioInfo.writerIndex, err
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

		index := ioInfo.reader.NextIndex() - 1
		callback(ioInfo.buffer[:n], index, nil)
	}()
}

func (f *FileController) SeekIndex(fileId, index uint64, callback func(error)) {
	ioInfo, ok := f.fileIdMap[fileId]
	if !ok {
		callback(fmt.Errorf("Unknown file ID: %d", fileId))
		return
	}

	go func() {
		callback(ioInfo.reader.SeekIndex(index))
	}()
}

func (f *FileController) ValidateLeader(fileName string, index uint64) bool {
	writer := f.ioProvider.ProvideWriter(fileName)
	return writer.LastIndex() <= index
}
