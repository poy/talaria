package broker

import (
	"sync"

	"github.com/apoydence/talaria/logging"
)

type connInfo struct {
	fileName string
	conn     *Connection
}

type Client struct {
	log logging.Logger

	syncFileIds sync.RWMutex
	fileIds     map[uint64]*connInfo
	fileNames   map[string]uint64
	fetcher     *ConnectionFetcher
}

func NewClient(URLs ...string) (*Client, error) {
	log := logging.Log("Client")
	log.Debug("Broker List: %v", URLs)

	fetcher, err := NewConnectionFetcher(nil, URLs...)
	if err != nil {
		return nil, err
	}

	return &Client{
		log:       log,
		fetcher:   fetcher,
		fileIds:   make(map[uint64]*connInfo),
		fileNames: make(map[string]uint64),
	}, nil
}

func (c *Client) Close() {
	c.fetcher.Close()
}

func (c *Client) FetchWriter(fileName string) (*Writer, error) {
	return NewWriter(fileName, NewConnectionFetcherWrapper(c.fetcher)), nil
}

func (c *Client) FetchReader(fileName string) (*Reader, error) {
	return NewReader(fileName, NewConnectionFetcherWrapper(c.fetcher)), nil
}

func (c *Client) LeaderOf(fileId uint64) (string, error) {
	panic("TODO")
}
