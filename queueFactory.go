package talaria

import (
	"io"
	"sync"
)

type QueueFactory struct {
	queueMap map[string]io.ReadWriteCloser
	locker   sync.Locker
}

func NewQueueFactory() *QueueFactory {
	return &QueueFactory{
		queueMap: make(map[string]io.ReadWriteCloser),
		locker:   &sync.Mutex{},
	}
}

func (qf *QueueFactory) Fetch(queueName string) io.ReadWriteCloser {
	defer qf.locker.Unlock()
	qf.locker.Lock()
	var queue io.ReadWriteCloser
	var ok bool
	if queue, ok = qf.queueMap[queueName]; !ok {
		queue = NewQueue()
		qf.queueMap[queueName] = queue
	}

	return queue
}

func (qf *QueueFactory) Remove(queueName string) {
	defer qf.locker.Unlock()
	qf.locker.Lock()
	delete(qf.queueMap, queueName)
}
