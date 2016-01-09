package broker

import "github.com/apoydence/talaria/logging"

type connInfo struct {
	fileName string
	conn     *Connection
}

type Client struct {
	log     logging.Logger
	fetcher *ConnectionFetcher
}

func NewClient(URLs ...string) (*Client, error) {
	log := logging.Log("Client")
	log.Debug("Broker List: %v", URLs)

	fetcher, err := NewConnectionFetcher(nil, URLs...)
	if err != nil {
		return nil, err
	}

	return &Client{
		log:     log,
		fetcher: fetcher,
	}, nil
}

func (c *Client) Close() {
	c.fetcher.Close()
}

func (c *Client) CreateFile(fileName string) error {
	_, _, err := c.fetcher.Fetch(fileName, true)
	return err
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
