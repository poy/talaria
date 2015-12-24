package broker_test

import "github.com/apoydence/talaria/broker"

type mockReaderFetcher struct {
	nameCh   chan string
	readerCh chan *broker.Reader
	errCh    chan error
}

func newMockReaderFetcher() *mockReaderFetcher {
	return &mockReaderFetcher{
		nameCh:   make(chan string, 100),
		readerCh: make(chan *broker.Reader, 100),
		errCh:    make(chan error, 100),
	}
}

func (m *mockReaderFetcher) FetchReader(name string) (*broker.Reader, error) {
	m.nameCh <- name
	return <-m.readerCh, <-m.errCh
}
