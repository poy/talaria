package files

import (
	"io"
	"net/http"
	"sync"

	"github.com/apoydence/talaria/logging"
)

type ReadIndexSeeker interface {
	io.Reader
	SeekIndex(uint64) error
	NextIndex() int64
}

type InitableWriter interface {
	io.Writer
	InitWriteIndex(index int64, data []byte) (int64, error)
}

type HttpStarter interface {
	Start(handler http.Handler)
}

type ReplicatedFileLeader struct {
	log       logging.Logger
	writer    InitableWriter
	lenBuffer []byte
	preData   ReadIndexSeeker

	syncClient   sync.RWMutex
	clientWriter io.Writer
}

func NewReplicatedFileLeader(writer InitableWriter, preData ReadIndexSeeker) *ReplicatedFileLeader {
	r := &ReplicatedFileLeader{
		log:       logging.Log("ReplicatedFileLeader"),
		writer:    writer,
		lenBuffer: make([]byte, 4),
		preData:   preData,
	}

	return r
}

func (r *ReplicatedFileLeader) Write(data []byte) (int, error) {
	r.writeToClient(data)

	n, err := r.writer.Write(data)
	if err != nil {
		r.log.Panic("Unable to write data", err)
	}

	return n, err
}

func (r *ReplicatedFileLeader) UpdateWriter(writer InitableWriter) {
	r.syncClient.Lock()
	defer r.syncClient.Unlock()

	r.clientWriter = writer

	buffer := make([]byte, 1024)
	n, err := r.preData.Read(buffer)
	if err == io.EOF {
		return
	}

	if err != nil {
		r.log.Panic("Failed to read from pre-data", err)
	}

	nextIndex := r.preData.NextIndex()
	index, err := writer.InitWriteIndex(nextIndex-1, buffer[:n])
	if err != nil {
		r.log.Panic("Unable to init write index", err)
	}

	r.writePreData(uint64(index + 1))
}

func (r *ReplicatedFileLeader) InitWriteIndex(index int64, data []byte) (int64, error) {
	return r.writer.InitWriteIndex(index, data)
}

func (r *ReplicatedFileLeader) getWriter() io.Writer {
	r.syncClient.RLock()
	defer r.syncClient.RUnlock()
	return r.clientWriter
}

func (r *ReplicatedFileLeader) writeToClient(data []byte) {
	clientWriter := r.getWriter()
	if clientWriter == nil {
		return
	}

	if _, err := clientWriter.Write(data); err != nil {
		r.log.Panic("Error while writing to client writer", err)
	}
}

func (r *ReplicatedFileLeader) writePreData(index uint64) {
	err := r.preData.SeekIndex(index)
	if err != nil && err != io.EOF {
		r.log.Panicf("Unable to seek to %d: %v", index, err)
	}
	buffer := make([]byte, 1024)

	for {
		n, err := r.preData.Read(buffer)
		if err == io.EOF && n <= 0 {
			return
		}

		if err != nil {
			r.log.Panic("Unable to read from pre-data", err)
		}

		if _, err = r.clientWriter.Write(buffer[:n]); err != nil {
			r.log.Panic("Unable to write to client writer", err)
		}
	}
}
