package broker

import (
	"fmt"
	"github.com/apoydence/talaria/logging"
	"net/url"
	"sync"
)

type ConnectionFetcher struct {
	log logging.Logger

	syncConns    sync.RWMutex
	conns        []*Connection
	nextFetchIdx uint64
}

func NewConnectionFetcher(URLs ...string) (*ConnectionFetcher, error) {
	log := logging.Log("ConnectionFetcher")
	log.Debug("Broker List: %v", URLs)

	var conns []*Connection
	for _, URL := range URLs {
		verifyUrl(URL, log)
		conn, err := NewConnection(URL)
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}

	return &ConnectionFetcher{
		log:   log,
		conns: conns,
	}, nil
}

func (c *ConnectionFetcher) Fetch(fileName string) (*Connection, uint64, error) {
	c.syncConns.Lock()
	c.syncConns.Unlock()

	fileId := c.getNextFetchIdx()
	conn := c.roundRobinConns(fileId)

	err := conn.FetchFile(fileId, fileName)
	if err == nil {
		return conn, fileId, nil
	}

	if err.Uri == "" {
		return nil, 0, fmt.Errorf(err.errMessage)
	}

	conn = c.fetchConnection(err.Uri)
	if conn == nil {
		conn = c.createConnection(err.Uri)
		c.conns = append(c.conns, conn)
	}

	err = conn.FetchFile(fileId, fileName)
	if err == nil {
		return conn, fileId, nil
	}

	return nil, 0, fmt.Errorf(err.Error())
}

func (c *ConnectionFetcher) Close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}

func (c *ConnectionFetcher) getNextFetchIdx() uint64 {
	defer func() {
		c.nextFetchIdx++
	}()
	return c.nextFetchIdx
}

func (c *ConnectionFetcher) createConnection(URL string) *Connection {
	conn, err := NewConnection(URL)
	if err != nil {
		c.log.Panicf("Unable to create connection to %s: %v", URL, err)
	}

	return conn
}

func (c *ConnectionFetcher) roundRobinConns(fileId uint64) *Connection {
	return c.conns[int(fileId)%len(c.conns)]
}

func (c *ConnectionFetcher) fetchConnection(URL string) *Connection {
	c.log.Debug("Fetching connection for %s", URL)
	for _, info := range c.conns {
		if info.URL == URL {
			c.log.Debug("Found connection for %s", URL)
			return info
		}
	}
	return nil
}

func verifyUrl(URL string, log logging.Logger) {
	u, _ := url.Parse(URL)
	if u == nil || u.Host == "" {
		log.Panicf("Invalid URL: %s", URL)
	}
}
