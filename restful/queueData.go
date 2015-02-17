package restful

import (
	"github.com/apoydence/talaria"
	"net/http"
)

type QueueData struct {
	QueueName string             `json:"queueName"`
	Buffer    talaria.BufferSize `json:"bufferSize"`
}

func NewQueueData(name string, size talaria.BufferSize) QueueData {
	return QueueData{
		QueueName: name,
		Buffer:    size,
	}
}

type connectToQueueData struct {
	ReadQueue  string
	WriteQueue string
}

func newConnectToQueueData(header http.Header) connectToQueueData {
	return connectToQueueData{
		ReadQueue:  header.Get("read"),
		WriteQueue: header.Get("write"),
	}
}
