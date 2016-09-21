package ringbuffer

import (
	"io"
	"sync/atomic"
	"unsafe"
)

type RingBuffer struct {
	buffer     []unsafe.Pointer
	writeIndex uint64
	Size       int
}

type bucket struct {
	data []byte
	seq  uint64
}

var New = func(size int) *RingBuffer {
	b := &RingBuffer{
		Size:   size,
		buffer: make([]unsafe.Pointer, size),
	}
	b.writeIndex = ^b.writeIndex
	return b
}

func (b *RingBuffer) WriteTo(data []byte) (uint64, error) {
	writeIndex := atomic.AddUint64(&b.writeIndex, 1)
	idx := writeIndex % uint64(len(b.buffer))
	newBucket := &bucket{
		data: data,
		seq:  writeIndex,
	}

	atomic.StorePointer(&b.buffer[idx], unsafe.Pointer(newBucket))
	return idx, nil
}

func (b *RingBuffer) ReadAt(readIndex uint64) ([]byte, uint64, error) {
	idx := readIndex % uint64(len(b.buffer))
	result := (*bucket)(atomic.LoadPointer(&b.buffer[idx]))

	if result == nil || result.seq < readIndex {
		return nil, 0, io.EOF
	}

	return result.data, result.seq, nil
}
