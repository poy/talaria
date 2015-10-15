package broker

type Conn interface {
	WriteToFile(fileId uint64, data []byte) (int64, error)
}

type ConnectionWrapper struct {
	fileId uint64
	conn   Conn
}

func NewConnectionWrapper(fileId uint64, conn Conn) *ConnectionWrapper {
	return &ConnectionWrapper{
		fileId: fileId,
		conn:   conn,
	}
}

func (c *ConnectionWrapper) Write(data []byte) (int, error) {
	_, err := c.conn.WriteToFile(c.fileId, data)
	return len(data), err
}
