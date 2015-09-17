package broker

import "sync"

type connWrapper interface {
	FetchFile(fileId uint64, name string) *FetchFileError
	WriteToFile(fileId uint64, data []byte) (int64, error)
	ReadFromFile(fileId uint64) ([]byte, error)
	Close()
}

type syncedConnection struct {
	lock sync.Mutex
	conn connWrapper
}

func newSyncedConnection(conn connWrapper) *syncedConnection {
	return &syncedConnection{
		conn: conn,
	}
}

func (s *syncedConnection) FetchFile(fileId uint64, name string) *FetchFileError {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.conn.FetchFile(fileId, name)
}

func (s *syncedConnection) WriteToFile(fileId uint64, data []byte) (int64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.conn.WriteToFile(fileId, data)
}

func (s *syncedConnection) ReadFromFile(fileId uint64) ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.conn.ReadFromFile(fileId)
}

func (s *syncedConnection) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.conn.Close()
}
