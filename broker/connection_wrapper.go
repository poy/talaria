package broker

type Conn interface {
	WriteToFile(fileId uint64, data []byte) (int64, *ConnectionError)
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
	if err != nil {
		return 0, nil
	}
	return len(data), nil
}
