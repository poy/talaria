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

	conns []*connInfo
}

func NewClient(URLs ...string) (*Client, error) {
	log := logging.Log("Client")
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
		log:     log,
		conns:   conns,
		fileIds: make(map[uint64]*connInfo),
	}, nil
}

func (c *Client) FetchFile(name string) (uint64, error) {
	fileId := c.getNextFetchIdx()
	conn := c.conns[int(fileId)%len(c.conns)]
	err := conn.conn.FetchFile(fileId, name)
	if err == nil {
		c.saveFileId(fileId, conn)
		return fileId, nil
	}

	if err.Uri == "" {
		return 0, fmt.Errorf(err.errMessage)
	}

	conn = c.fetchConnection(err.Uri)
	if conn == nil {
		return 0, fmt.Errorf("Unknown broker: %s", err.Uri)
	}

	err = conn.conn.FetchFile(fileId, name)
	if err == nil {
		c.saveFileId(fileId, conn)
		return fileId, nil
	}

	return 0, fmt.Errorf(err.Error())
}

func (c *Client) Close() {
	for _, info := range c.conns {
		info.conn.Close()
	}
}

func (c *Client) WriteToFile(fileId uint64, data []byte) (int64, error) {
	conn := c.fetchConnectionById(fileId)
	if conn == nil {
		return 0, fmt.Errorf("Unknown file ID: %d", fileId)
	}

	return conn.conn.WriteToFile(fileId, data)
}

func (c *Client) ReadFromFile(fileId uint64) ([]byte, error) {
	conn := c.fetchConnectionById(fileId)
	if conn == nil {
		return nil, fmt.Errorf("Unknown file ID: %d", fileId)
	}

	return conn.conn.ReadFromFile(fileId)
}

func (c *Client) saveFileId(fileId uint64, conn *connInfo) {
	c.syncFileIds.Lock()
	defer c.syncFileIds.Unlock()
	c.fileIds[fileId] = conn
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
	for _, info := range c.conns {
		if info.URL[2:] == URL[4:] {
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
