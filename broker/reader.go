package broker

type ReadConnection interface {
	ReadFromFile(fileId uint64) ([]byte, int64, *ConnectionError)
	SeekIndex(fileId, index uint64) *ConnectionError
}

type Reader struct {
	fileId uint64
	conn   ReadConnection
}

func NewReader(fileId uint64, conn ReadConnection) *Reader {
	return &Reader{
		fileId: fileId,
		conn:   conn,
	}
}

func (r *Reader) ReadFromFile() ([]byte, int64, *ConnectionError) {
	return r.conn.ReadFromFile(r.fileId)
}

func (r *Reader) SeekIndex(index uint64) *ConnectionError {
	return r.conn.SeekIndex(r.fileId, index)
}
