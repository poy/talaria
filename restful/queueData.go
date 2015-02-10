package restful

import (
	"github.com/apoydence/talaria"
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
