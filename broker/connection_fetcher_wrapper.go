package broker

type ConnectionFetcherWrapper struct {
	fetcher *ConnectionFetcher
}

func NewConnectionFetcherWrapper(fetcher *ConnectionFetcher) *ConnectionFetcherWrapper {
	return &ConnectionFetcherWrapper{
		fetcher: fetcher,
	}
}

func (w *ConnectionFetcherWrapper) FetchWriter(fileName string) (WriteConnection, uint64, error) {
	return w.fetcher.Fetch(fileName, false)
}

func (w *ConnectionFetcherWrapper) FetchReader(fileName string) (ReadConnection, uint64, error) {
	return w.fetcher.Fetch(fileName, false)
}
