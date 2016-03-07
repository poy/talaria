package client

import "github.com/apoydence/talaria/common"

type ReadConnection interface {
	ReadFromFile(fileId uint64) ([]byte, int64, *common.ConnectionError)
	SeekIndex(fileId, index uint64) *common.ConnectionError
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

func (r *Reader) ReadFromFile() ([]byte, int64, *common.ConnectionError) {
	fileId, conn, fcErr := r.fetchConnection()
	if fcErr != nil {
		return nil, 0, common.NewConnectionError(fcErr.Error(), "", "", false)
	}

	data, index, err := conn.ReadFromFile(fileId)

	if err != nil && err.WebsocketError {
		r.conn = nil
		return r.ReadFromFile()
	}

	return data, index, err
}

func (r *Reader) SeekIndex(index uint64) *common.ConnectionError {
	fileId, conn, fcErr := r.fetchConnection()
	if fcErr != nil {
		return common.NewConnectionError(fcErr.Error(), "", "", false)
	}

	err := conn.SeekIndex(fileId, index)

	if err != nil && err.WebsocketError {
		r.conn = nil
		return r.SeekIndex(index)
	}

	return err
}

func (r *Reader) fetchConnection() (uint64, ReadConnection, error) {
	if r.conn == nil {
		var err error
		r.conn, r.fileId, err = r.fetcher.FetchReader(r.fileName)

		if err != nil {
			return 0, nil, err
		}
	}

	return r.fileId, r.conn, nil
}
