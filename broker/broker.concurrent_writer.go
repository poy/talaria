package broker

import (
	"sync"

	"github.com/gorilla/websocket"
)

type concurrentWriter struct {
	lock sync.Mutex
	conn *websocket.Conn
}

func newConcurrentWriter(conn *websocket.Conn) *concurrentWriter {
	return &concurrentWriter{
		conn: conn,
	}
}

func (c *concurrentWriter) Write(data []byte) (int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	err := c.conn.WriteMessage(websocket.BinaryMessage, data)
	return 0, err
}
