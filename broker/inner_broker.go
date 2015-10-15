package broker

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/apoydence/talaria/logging"
)

type InnerBroker struct {
	log        logging.Logger
	nextFileId uint64

	connLock sync.Mutex
	connMap  map[string]*Connection
}

func NewInnerBroker() *InnerBroker {
	return &InnerBroker{
		log:     logging.Log("InnerBroker"),
		connMap: make(map[string]*Connection),
	}
}

func (i *InnerBroker) ProvideConn(name, addr string) io.Writer {
	conn := i.fetchConnection(addr + InnerEndpoint)
	fileId := i.fetchFileId()

	i.log.Debug("Fetching inner connection for %s from %s", name, addr)
	ffErr := conn.FetchFile(fileId, name)
	if ffErr != nil {
		i.log.Panicf("Unable to fetch file from inner broker (%s): %v", addr, ffErr)
	}

	return NewConnectionWrapper(fileId, conn)
}

func (i *InnerBroker) fetchConnection(addr string) *Connection {
	i.connLock.Lock()
	defer i.connLock.Unlock()

	if conn, ok := i.connMap[addr]; ok {
		return conn
	}

	conn, err := NewConnection(addr)
	if err != nil {
		i.log.Panicf("Unable to connect to inner broker (%s): %v", addr, err)
	}

	i.connMap[addr] = conn
	return conn
}

func (i *InnerBroker) fetchFileId() uint64 {
	return atomic.AddUint64(&i.nextFileId, 1)
}
