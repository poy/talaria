package talaria

type queue chan []byte
type BufferSize int

const AnyBufferSize BufferSize = -1

func NewQueue(size BufferSize) Queue {
	return queue(make(chan []byte, size))
}

func (q queue) BufferSize() BufferSize {
	return BufferSize(cap(q))
}

func (q queue) Read() []byte {
	result, ok := <-q
	if ok {
		return result
	}
	return nil
}

func (q queue) ReadAsync() []byte {
	select {
	case result, ok := <-q:
		if ok {
			return result
		}
		return nil
	default:
		return nil
	}
}

func (q queue) Write(data []byte) bool {
	var notClosed bool
	func() {
		defer func() {
			notClosed = recover() == nil
		}()
		q <- data
	}()
	return notClosed
}

func (q queue) Close() {
	close(q)
}
