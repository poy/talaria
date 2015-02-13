package talaria

import (
	"sync"
)

type queue struct {
	ch   chan []byte
	once sync.Once
}

type BufferSize int

const AnyBufferSize BufferSize = -1

func NewQueue(size BufferSize) Queue {
	return &queue{
		ch:   make(chan []byte, size),
		once: sync.Once{},
	}
}

func (q *queue) BufferSize() BufferSize {
	return BufferSize(cap(q.ch))
}

func (q *queue) Read() []byte {
	result, ok := <-q.ch
	if ok {
		return result
	}
	return nil
}

func (q *queue) ReadAsync() []byte {
	select {
	case result, ok := <-q.ch:
		if ok {
			return result
		}
		return nil
	default:
		return nil
	}
}

func (q *queue) Write(data []byte) bool {
	var notClosed bool
	func() {
		defer func() {
			notClosed = recover() == nil
		}()
		q.ch <- data
	}()
	return notClosed
}

func (q *queue) Close() {
	q.once.Do(func() {
		close(q.ch)
	})
}
