package client_test

import "github.com/apoydence/talaria/client"

type mockReaderFetcher struct {
	nameCh   chan string
	readerCh chan *client.Reader
	errCh    chan error
}

func newMockReaderFetcher() *mockReaderFetcher {
	return &mockReaderFetcher{
		nameCh:   make(chan string, 100),
		readerCh: make(chan *client.Reader, 100),
		errCh:    make(chan error, 100),
	}
}

func (m *mockReaderFetcher) FetchReader(name string) (*client.Reader, error) {
	m.nameCh <- name
	return <-m.readerCh, <-m.errCh
}
