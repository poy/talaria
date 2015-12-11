package broker

type DirectConnection interface {
	ReadFromFile(fileId uint64) ([]byte, int64, *ConnectionError)
}

type Reader struct {
	fileId uint64
	conn   DirectConnection
}

func NewReader(fileId uint64, conn DirectConnection) *Reader {
	return &Reader{
		fileId: fileId,
		conn:   conn,
	}
}

func (r *Reader) ReadFromFile() ([]byte, int64, *ConnectionError) {
	return r.conn.ReadFromFile(r.fileId)
}
