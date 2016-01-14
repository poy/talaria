package broker_test

import "github.com/apoydence/talaria/broker"

type mockReadConnectionFetcher struct {
	fileNameCh       chan string
	readConnectionCh chan broker.ReadConnection
	fileIdCh         chan uint64
	errCh            chan error

	impeachUrlCh chan string
	impeacherCh  chan broker.Impeacher
}

func newMockReadConnectionFetcher() *mockReadConnectionFetcher {
	return &mockReadConnectionFetcher{
		fileNameCh:       make(chan string, 100),
		readConnectionCh: make(chan broker.ReadConnection, 100),
		fileIdCh:         make(chan uint64, 100),
		errCh:            make(chan error, 100),

		impeachUrlCh: make(chan string, 100),
		impeacherCh:  make(chan broker.Impeacher, 100),
	}
}

func (m *mockReadConnectionFetcher) FetchReader(fileName string) (broker.ReadConnection, uint64, error) {
	m.fileNameCh <- fileName
	return <-m.readConnectionCh, <-m.fileIdCh, <-m.errCh
}

func (m *mockReadConnectionFetcher) FetchImpeacher(URL string) broker.Impeacher {
	m.impeachUrlCh <- URL
	return <-m.impeacherCh
}
