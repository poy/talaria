package client

import "github.com/apoydence/talaria/common"

type WriteConnection interface {
	WriteToFile(fileId uint64, data []byte) (int64, *common.ConnectionError)
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

func (w *Writer) WriteToFile(data []byte) (int64, *common.ConnectionError) {
	fileId, conn, fcErr := w.fetchConnection()
	if fcErr != nil {
		return 0, common.NewConnectionError(fcErr.Error(), "", "", false)
	}

	index, err := conn.WriteToFile(fileId, data)

	if err != nil && err.WebsocketError {
		w.conn = nil
		return w.WriteToFile(data)
	}

	return index, err
}

func (w *Writer) fetchConnection() (uint64, WriteConnection, error) {
	if w.conn == nil {
		var err error
		w.conn, w.fileId, err = w.fetcher.FetchWriter(w.fileName)
		if err != nil {
			return 0, nil, err
		}
	}

	return w.fileId, w.conn, nil
}
