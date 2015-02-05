package talaria

type Queue chan []byte

func NewQueue() Queue {
	return make(chan []byte)
}

func (q Queue) Read(buffer []byte) (int, error) {
	panic("Not implemented")
}

func (q Queue) Write(data []byte) (int, error) {
	panic("Not implemented")
}

func (q Queue) Close() error {
	panic("Not implemented")
}
