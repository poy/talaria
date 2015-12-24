package broker

import (
	"fmt"
	"net/url"
	"sync"

	"github.com/apoydence/talaria/logging"
)

type connInfo struct {
	fileName string
	conn     *Connection
}

type Client struct {
	log          logging.Logger
	syncFetchIdx sync.Mutex
	nextFetchIdx uint64

	syncFileIds sync.RWMutex
	fileIds     map[uint64]*connInfo
	fileNames   map[string]uint64

	syncConns sync.RWMutex
	conns     []*Connection
}

func NewClient(URLs ...string) (*Client, error) {
	log := logging.Log("Client")
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

	return &Client{
		log:       log,
		conns:     conns,
		fileIds:   make(map[uint64]*connInfo),
		fileNames: make(map[string]uint64),
	}, nil
}

func (c *Client) Close() {
	c.syncConns.RLock()
	defer c.syncConns.RUnlock()

	for _, info := range c.conns {
		info.conn.Close()
	}
}

func (c *Client) WriteToFile(fileName string, data []byte) (int64, error) {
	fileId, ok := c.fetchIdByName(fileName)
	if !ok {
		if _, err := c.fetchFile(fileName); err != nil {
			return 0, err
		}

		return c.WriteToFile(fileName, data)
	}

	conn := c.fetchConnectionById(fileId)
	if conn == nil {
		return 0, fmt.Errorf("Unknown file ID: %d", fileId)
	}

	return conn.conn.WriteToFile(fileId, data)
}

func (c *Client) FetchReader(fileName string) (*Reader, error) {
	fileId, err := c.fetchFile(fileName)
	if err != nil {
		return nil, err
	}

	conn := c.fetchConnectionById(fileId)
	if conn == nil {
		return nil, fmt.Errorf("Unknown file ID: %d", fileId)
	}

	c.log.Debug("Creating new Reader (fileId=%d) for %s", fileId, fileName)
	return NewReader(fileId, conn.conn), nil
}

func (c *Client) LeaderOf(fileId uint64) (string, error) {
	conn := c.fetchConnectionById(fileId)
	if conn == nil {
		return "", fmt.Errorf("Unknown file ID: %d", fileId)
	}

	return conn.conn.URL, nil
}

func (c *Client) fetchFile(name string) (uint64, error) {
	c.syncConns.Lock()
	c.syncConns.Unlock()

	fileId := c.getNextFetchIdx()
	conn := c.roundRobinConns(fileId)

	err := conn.FetchFile(fileId, name)
	if err == nil {
		c.saveFileId(fileId, name, conn)
		return fileId, nil
	}

	if err.Uri == "" {
		return 0, fmt.Errorf(err.errMessage)
	}

	conn = c.fetchConnection(err.Uri)
	if conn == nil {
		conn = c.createConnection(err.Uri)
		c.conns = append(c.conns, conn)
	}

	err = conn.FetchFile(fileId, name)
	if err == nil {
		c.saveFileId(fileId, name, conn)
		return fileId, nil
	}

	return 0, fmt.Errorf(err.Error())
}

func (c *Client) roundRobinConns(fileId uint64) *Connection {
	return c.conns[int(fileId)%len(c.conns)]
}

func (c *Client) createConnection(URL string) *Connection {
	conn, err := NewConnection(URL)
	if err != nil {
		c.log.Panicf("Unable to create connection to %s: %v", URL, err)
	}

	return conn
}

func (c *Client) saveFileId(fileId uint64, name string, conn *Connection) {
	c.syncFileIds.Lock()
	defer c.syncFileIds.Unlock()
	c.fileIds[fileId] = &connInfo{
		conn:     conn,
		fileName: name,
	}
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

func (c *Client) fetchConnection(URL string) *Connection {
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
