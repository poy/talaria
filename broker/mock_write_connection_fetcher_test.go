package broker_test

import "github.com/apoydence/talaria/broker"

type mockWriteConnectionFetcher struct {
	fileNameCh        chan string
	writeConnectionCh chan broker.WriteConnection
	fileIdCh          chan uint64
	errCh             chan error
}

func newMockWriteConnectionFetcher() *mockWriteConnectionFetcher {
	return &mockWriteConnectionFetcher{
		fileNameCh:        make(chan string, 100),
		writeConnectionCh: make(chan broker.WriteConnection, 100),
		fileIdCh:          make(chan uint64, 100),
		errCh:             make(chan error, 100),
	}
}

func (m *mockWriteConnectionFetcher) FetchWriter(fileName string) (broker.WriteConnection, uint64, error) {
	m.fileNameCh <- fileName
	return <-m.writeConnectionCh, <-m.fileIdCh, <-m.errCh
}
