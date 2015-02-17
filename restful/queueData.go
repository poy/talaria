package restful

import (
	"errors"
	"github.com/apoydence/talaria"
	"net/http"
	"net/url"
	"strconv"
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

func ParseQueueData(values url.Values) (QueueData, error) {
	name := values.Get("queueName")
	size := values.Get("bufferSize")
	if len(name) == 0 || len(size) == 0 {
		return QueueData{}, errors.New("queueName and bufferSize are both required")
	}
	i, err := strconv.Atoi(size)
	if err != nil {
		return QueueData{}, err
	}
	return QueueData{
		QueueName: name,
		Buffer:    talaria.BufferSize(i),
	}, nil
}

func (qd QueueData) ToUrlValues() url.Values {
	return url.Values{
		"queueName":  {qd.QueueName},
		"bufferSize": {strconv.Itoa(int(qd.Buffer))},
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
