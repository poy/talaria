package broker

type ReadConnection interface {
	ReadFromFile(fileId uint64) ([]byte, int64, *ConnectionError)
	SeekIndex(fileId, index uint64) *ConnectionError
}

type ReadConnectionFetcher interface {
	FetchReader(fileName string) (ReadConnection, uint64, error)
}

type Reader struct {
	fetcher  ReadConnectionFetcher
	fileName string
	fileId   uint64
	conn     ReadConnection
}

func NewReader(fileName string, fetcher ReadConnectionFetcher) *Reader {
	return &Reader{
		fileName: fileName,
		fetcher:  fetcher,
	}
}

func (r *Reader) ReadFromFile() ([]byte, int64, *ConnectionError) {
	fileId, conn := r.fetchConnection()
	data, index, err := conn.ReadFromFile(fileId)

	if err != nil && err.WebsocketError {
		r.conn = nil
		return r.ReadFromFile()
	}

	return data, index, err
}

func (r *Reader) SeekIndex(index uint64) *ConnectionError {
	fileId, conn := r.fetchConnection()
	err := conn.SeekIndex(fileId, index)

	if err != nil && err.WebsocketError {
		r.conn = nil
		return r.SeekIndex(index)
	}

	return err
}

func (r *Reader) fetchConnection() (uint64, ReadConnection) {
	if r.conn == nil {
		r.conn, r.fileId, _ = r.fetcher.FetchReader(r.fileName)
	}

	return r.fileId, r.conn
}
