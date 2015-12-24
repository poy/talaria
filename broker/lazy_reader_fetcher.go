package broker

import (
	"github.com/apoydence/talaria/logging"
	"sync"
)

type LazyReaderFetcher struct {
	log  logging.Logger
	addr string

	lock   sync.Mutex
	client *Client
}

func NewLazyReaderFetcher(addr string) *LazyReaderFetcher {
	return &LazyReaderFetcher{
		log:  logging.Log("LazyReaderFetcher"),
		addr: addr,
	}
}

func (l *LazyReaderFetcher) FetchReader(name string) (*Reader, error) {
	return l.fetchClient().FetchReader(name)
}

func (l *LazyReaderFetcher) fetchClient() *Client {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.client == nil {
		l.log.Debug("Lazy creation of client: %s", l.addr)
		l.client = l.createClient()
	}

	return l.client
}

func (l *LazyReaderFetcher) createClient() *Client {
	client, err := NewClient(l.addr)
	if err != nil {
		l.log.Panic("Unable to create client", err)
	}

	return client
}
