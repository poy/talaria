package broker

import (
	"fmt"
	"io"
)

type IoProvider interface {
	ProvideWriter(name string) io.Writer
	ProvideReader(name string) io.Reader
}

type Orchestrator interface {
	FetchLeader(name string) (string, bool)
}

type ioInfo struct {
	writer       io.Writer
	reader       io.Reader
	writerOffset int64
	buffer       []byte
}

type FileController struct {
	fileNameMap  map[string]uint64
	fileIdMap    map[uint64]*ioInfo
	nextFileId   uint64
	ioProvider   IoProvider
	orchestrator Orchestrator
}

func NewFileController(ioProvider IoProvider, orchestrator Orchestrator) *FileController {
	return &FileController{
		fileNameMap:  make(map[string]uint64),
		fileIdMap:    make(map[uint64]*ioInfo),
		ioProvider:   ioProvider,
		orchestrator: orchestrator,
	}
}

func (f *FileController) FetchFile(name string) (uint64, *FetchFileError) {
	fileId, ok := f.fileNameMap[name]
	if !ok {
		uri, local := f.orchestrator.FetchLeader(name)
		if !local {
			return 0, NewFetchFileError("Redirect to the correct broker", uri)
		}

		fileId = f.nextId()
		f.fileNameMap[name] = fileId
		f.fileIdMap[fileId] = &ioInfo{
			writer: f.ioProvider.ProvideWriter(name),
			reader: f.ioProvider.ProvideReader(name),
			buffer: make([]byte, 1024),
		}
	}
	return fileId, nil
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

func (f *FileController) ReadFromFile(fileId uint64) ([]byte, error) {
	ioInfo, ok := f.fileIdMap[fileId]
	if !ok {
		return nil, fmt.Errorf("Unknown file ID: %d", fileId)
	}

	n, err := ioInfo.reader.Read(ioInfo.buffer)
	return ioInfo.buffer[:n], err
}

func (f *FileController) nextId() uint64 {
	f.nextFileId++
	return f.nextFileId
}
