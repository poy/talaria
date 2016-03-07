package broker

import (
	"sync"

	"github.com/apoydence/talaria/broker/leadervalidator"
	"github.com/apoydence/talaria/client"
	"github.com/apoydence/talaria/logging"
)

type LazyReaderFetcher struct {
	log  logging.Logger
	addr string

	lock    sync.Mutex
	fetcher *client.ConnectionFetcher
}

func NewLazyReaderFetcher(addr string) *LazyReaderFetcher {
	return &LazyReaderFetcher{
		log:  logging.Log("LazyReaderFetcher"),
		addr: addr,
	}
}

func (l *LazyReaderFetcher) FetchReader(name string) (client.ReadConnection, uint64, error) {
	return l.fetchFetcher().Fetch(name, false)
}

func (l *LazyReaderFetcher) FetchValidator(URL string) leadervalidator.Validator {
	conn, err := l.fetchFetcher().FetchConnection(URL)

	if err == client.BlacklistedErr {
		return nil
	}

	if err != nil {
		l.log.Panic("Unable to fetch connection.", err)
	}
	return conn
}

func (l *LazyReaderFetcher) fetchFetcher() *client.ConnectionFetcher {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.fetcher == nil {
		l.log.Debug("Lazy creation of ConnectionFetcher: %s", l.addr)
		l.fetcher = l.createFetcher()
	}

	return l.fetcher
}

func (l *LazyReaderFetcher) createFetcher() *client.ConnectionFetcher {
	fetcher, err := client.NewConnectionFetcher([]string{l.addr}, l.addr)
	if err != nil {
		l.log.Panic("Unable to create ConnectionFetcher", err)
	}

	return fetcher
}
