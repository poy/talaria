package talaria

import (
	"errors"
	"fmt"
	"sync"
)

type Queue interface {
	Read() []byte
	ReadAsync() []byte
	Write(data []byte) bool
	BufferSize() BufferSize
	Close()
}

type QueueFactory struct {
	queueMap          map[string]Queue
	locker            sync.Locker
	defaultBufferSize BufferSize
}

type QueueListing struct {
	Name string
	Q    Queue
}

func NewQueueFactory(defaultSize BufferSize) *QueueFactory {
	if defaultSize < 0 {
		panic("The default size must be a positive integer")
	}
	return &QueueFactory{
		queueMap:          make(map[string]Queue),
		locker:            &sync.Mutex{},
		defaultBufferSize: defaultSize,
	}
}

func (qf *QueueFactory) AddQueue(queueName string, size BufferSize) error {
	defer qf.locker.Unlock()
	qf.locker.Lock()
	var queue Queue
	var ok bool
	if queue, ok = qf.queueMap[queueName]; !ok {
		queue = NewQueue(qf.getSize(size))
		qf.queueMap[queueName] = queue
	} else {
		return errors.New(fmt.Sprintf("Queue with the name '%s' already exists", queueName))
	}

	return nil
}

func (qf *QueueFactory) Fetch(queueName string) Queue {
	defer qf.locker.Unlock()
	qf.locker.Lock()
	if queue, ok := qf.queueMap[queueName]; ok {
		return queue
	}
	return nil
}

func (qf *QueueFactory) Remove(queueName string) {
	defer qf.locker.Unlock()
	qf.locker.Lock()
	delete(qf.queueMap, queueName)
}

func (qf *QueueFactory) ListQueues() []QueueListing {
	qs := make([]QueueListing, 0)
	for k, v := range qf.queueMap {
		ql := QueueListing{
			Name: k,
			Q:    v,
		}
		qs = append(qs, ql)
	}
	return qs
}

func (qf *QueueFactory) getSize(size BufferSize) BufferSize {
	if size < 0 {
		return qf.defaultBufferSize
	}

	return size
}
