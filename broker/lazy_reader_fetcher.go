package broker

import (
	"github.com/apoydence/talaria/logging"
	"sync"
)

type LazyReaderFetcher struct {
	log  logging.Logger
	addr string

	lock    sync.Mutex
	fetcher *ConnectionFetcher
}

func NewLazyReaderFetcher(addr string) *LazyReaderFetcher {
	return &LazyReaderFetcher{
		log:  logging.Log("LazyReaderFetcher"),
		addr: addr,
	}
}

func (l *LazyReaderFetcher) FetchReader(name string) (ReadConnection, uint64, error) {
	return l.fetchFetcher().Fetch(name, false)
}

func (l *LazyReaderFetcher) fetchFetcher() *ConnectionFetcher {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.fetcher == nil {
		l.log.Debug("Lazy creation of ConnectionFetcher: %s", l.addr)
		l.fetcher = l.createFetcher()
	}

	return l.fetcher
}

func (l *LazyReaderFetcher) createFetcher() *ConnectionFetcher {
	fetcher, err := NewConnectionFetcher([]string{l.addr}, l.addr)
	if err != nil {
		l.log.Panic("Unable to create ConnectionFetcher", err)
	}

	return fetcher
}
