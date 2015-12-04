package broker

import (
	"fmt"
	"net/url"
	"sync"

	"github.com/apoydence/talaria/logging"
)

type connInfo struct {
	URL  string
	conn *Connection
}

type Client struct {
	log          logging.Logger
	syncFetchIdx sync.Mutex
	nextFetchIdx uint64

	syncFileIds sync.RWMutex
	fileIds     map[uint64]*connInfo
	fileNames   map[string]uint64

	conns []*connInfo
}

func NewClient(URLs ...string) (*Client, error) {
	log := logging.Log("Client")
	log.Debug("Broker List: %v", URLs)

	var conns []*connInfo
	for _, URL := range URLs {
		verifyUrl(URL, log)
		conn, err := NewConnection(URL)
		if err != nil {
			return nil, err
		}
		conns = append(conns, &connInfo{
			URL:  URL,
			conn: conn,
		})
	}

	return &Client{
		log:       log,
		conns:     conns,
		fileIds:   make(map[uint64]*connInfo),
		fileNames: make(map[string]uint64),
	}, nil
}

func (c *Client) FetchFile(name string) error {
	fileId := c.getNextFetchIdx()
	conn := c.conns[int(fileId)%len(c.conns)]
	err := conn.conn.FetchFile(fileId, name)
	if err == nil {
		c.saveFileId(fileId, name, conn)
		return nil
	}

	if err.Uri == "" {
		return fmt.Errorf(err.errMessage)
	}

	conn = c.fetchConnection(err.Uri)
	if conn == nil {
		return fmt.Errorf("Unknown broker: %s", err.Uri)
	}

	err = conn.conn.FetchFile(fileId, name)
	if err == nil {
		c.saveFileId(fileId, name, conn)
		return nil
	}

	return fmt.Errorf(err.Error())
}

func (c *Client) Close() {
	for _, info := range c.conns {
		info.conn.Close()
	}
}

func (c *Client) WriteToFile(fileName string, data []byte) (int64, error) {
	fileId, ok := c.fetchIdByName(fileName)
	if !ok {
		return 0, fmt.Errorf("Unknown file name: %s", fileName)
	}

	conn := c.fetchConnectionById(fileId)
	if conn == nil {
		return 0, fmt.Errorf("Unknown file ID: %d", fileId)
	}

	return conn.conn.WriteToFile(fileId, data)
}

func (c *Client) ReadFromFile(fileName string) ([]byte, int64, error) {
	fileId, ok := c.fetchIdByName(fileName)
	if !ok {
		return nil, 0, fmt.Errorf("Unknown file name: %s", fileName)
	}

	conn := c.fetchConnectionById(fileId)
	if conn == nil {
		return nil, 0, fmt.Errorf("Unknown file ID: %d", fileId)
	}

	return conn.conn.ReadFromFile(fileId)
}

func (c *Client) LeaderOf(fileId uint64) (string, error) {
	conn := c.fetchConnectionById(fileId)
	if conn == nil {
		return "", fmt.Errorf("Unknown file ID: %d", fileId)
	}

	return conn.URL, nil
}

func (c *Client) saveFileId(fileId uint64, name string, conn *connInfo) {
	c.syncFileIds.Lock()
	defer c.syncFileIds.Unlock()
	c.fileIds[fileId] = conn
	c.fileNames[name] = fileId
}

func (c *Client) fetchIdByName(fileName string) (uint64, bool) {
	c.syncFileIds.RLock()
	defer c.syncFileIds.RUnlock()
	id, ok := c.fileNames[fileName]
	return id, ok
}

func (c *Client) fetchConnectionById(fileId uint64) *connInfo {
	c.syncFileIds.RLock()
	defer c.syncFileIds.RUnlock()
	conn, ok := c.fileIds[fileId]
	if !ok {
		return nil
	}

	return conn
}

func (c *Client) getNextFetchIdx() uint64 {
	c.syncFetchIdx.Lock()
	defer func() {
		c.nextFetchIdx++
		c.syncFetchIdx.Unlock()
	}()
	return c.nextFetchIdx
}

func (c *Client) fetchConnection(URL string) *connInfo {
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
