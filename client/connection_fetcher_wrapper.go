package client

import "github.com/apoydence/talaria/logging"

type ConnectionFetcherWrapper struct {
	log     logging.Logger
	fetcher *ConnectionFetcher
}

func NewConnectionFetcherWrapper(fetcher *ConnectionFetcher) *ConnectionFetcherWrapper {
	return &ConnectionFetcherWrapper{
		log:     logging.Log("ConnectionFetcherWrapper"),
		fetcher: fetcher,
	}
}

func (w *ConnectionFetcherWrapper) FetchWriter(fileName string) (WriteConnection, uint64, error) {
	return w.fetcher.Fetch(fileName, false)
}

func (w *ConnectionFetcherWrapper) FetchReader(fileName string) (ReadConnection, uint64, error) {
	return w.fetcher.Fetch(fileName, false)
}
