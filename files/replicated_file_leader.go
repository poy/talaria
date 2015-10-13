package files

import (
	"io"
	"net/http"
	"sync"

	"github.com/apoydence/talaria/logging"
)

type HttpStarter interface {
	Start(handler http.Handler)
}

type ReadSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

type ReplicatedFileLeader struct {
	log       logging.Logger
	writer    io.Writer
	lenBuffer []byte
	preData   ReadSeekCloser

	syncClient   sync.RWMutex
	clientWriter io.Writer
}

func NewReplicatedFileLeader(writer io.Writer, preData ReadSeekCloser) *ReplicatedFileLeader {
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

func (r *ReplicatedFileLeader) UpdateWriter(writer io.Writer) {
	r.syncClient.Lock()
	defer r.syncClient.Unlock()

	r.clientWriter = writer
	r.writePreData()
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

func (r *ReplicatedFileLeader) writePreData() {
	r.preData.Seek(0, 0)
	buffer := make([]byte, 1024)

	for {
		n, err := r.preData.Read(buffer)
		if err == io.EOF && n == 0 {
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
