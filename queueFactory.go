package talaria

import (
	"errors"
	"sync"
)

type Queue interface {
	Read() []byte
	Write(data []byte) error
	BufferSize() BufferSize
}

type QueueFactory struct {
	queueMap          map[string]Queue
	locker            sync.Locker
	defaultBufferSize BufferSize
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

func (qf *QueueFactory) Fetch(queueName string, size BufferSize) (Queue, error) {
	defer qf.locker.Unlock()
	qf.locker.Lock()
	var queue Queue
	var ok bool
	if queue, ok = qf.queueMap[queueName]; !ok {
		queue = NewQueue(qf.getSize(size))
		qf.queueMap[queueName] = queue
	} else if size != AnyBufferSize && queue.BufferSize() != size {
		return nil, errors.New("Requested a differing buffer size")
	}

	return queue, nil
}

func (qf *QueueFactory) Remove(queueName string) {
	defer qf.locker.Unlock()
	qf.locker.Lock()
	delete(qf.queueMap, queueName)
}

func (qf *QueueFactory) getSize(size BufferSize) BufferSize {
	if size < 0 {
		return qf.defaultBufferSize
	}

	return size
}
