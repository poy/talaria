package broker

type WriteConnection interface {
	WriteToFile(fileId uint64, data []byte) (int64, *ConnectionError)
}

type WriteConnectionFetcher interface {
	FetchWriter(fileName string) (WriteConnection, uint64, error)
}

type Writer struct {
	fetcher  WriteConnectionFetcher
	fileName string

	fileId uint64
	conn   WriteConnection
}

func NewWriter(fileName string, fetcher WriteConnectionFetcher) *Writer {
	return &Writer{
		fileName: fileName,
		fetcher:  fetcher,
	}
}

func (w *Writer) WriteToFile(data []byte) (int64, *ConnectionError) {
	fileId, conn := w.fetchConnection()
	index, err := conn.WriteToFile(fileId, data)

	if err != nil && err.WebsocketError {
		w.conn = nil
		return w.WriteToFile(data)
	}

	return index, err
}

func (w *Writer) fetchConnection() (uint64, WriteConnection) {
	if w.conn == nil {
		w.conn, w.fileId, _ = w.fetcher.FetchWriter(w.fileName)
	}

	return w.fileId, w.conn
}
