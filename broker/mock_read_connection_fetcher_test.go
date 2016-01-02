package broker_test

import "github.com/apoydence/talaria/broker"

type mockReadConnectionFetcher struct {
	fileNameCh       chan string
	readConnectionCh chan broker.ReadConnection
	fileIdCh         chan uint64
	errCh            chan error
}

func newMockReadConnectionFetcher() *mockReadConnectionFetcher {
	return &mockReadConnectionFetcher{
		fileNameCh:       make(chan string, 100),
		readConnectionCh: make(chan broker.ReadConnection, 100),
		fileIdCh:         make(chan uint64, 100),
		errCh:            make(chan error, 100),
	}
}

func (m *mockReadConnectionFetcher) FetchReader(fileName string) (broker.ReadConnection, uint64, error) {
	m.fileNameCh <- fileName
	return <-m.readConnectionCh, <-m.fileIdCh, <-m.errCh
}
