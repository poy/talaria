package broker

type WriteConnection interface {
	WriteToFile(fileId uint64, data []byte) (int64, *ConnectionError)
}

type Writer struct {
	fileId uint64
	conn   WriteConnection
}

func NewWriter(fileId uint64, conn WriteConnection) *Writer {
	return &Writer{
		fileId: fileId,
		conn:   conn,
	}
}

func (r *Writer) WriteToFile(data []byte) (int64, *ConnectionError) {
	return r.conn.WriteToFile(r.fileId, data)
}
