package broker_test

type mockImpeacher struct {
	impeach chan string
}

func newMockImpeacher() *mockImpeacher {
	return &mockImpeacher{
		impeach: make(chan string, 100),
	}
}

func (m *mockImpeacher) Impeach(name string) {
	m.impeach <- name
}
