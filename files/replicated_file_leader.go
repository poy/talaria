package files

import (
	"encoding/binary"
	"io"
	"net/http"
	"sync"

	"github.com/apoydence/talaria/logging"
	"github.com/gorilla/websocket"
)

type HttpStarter interface {
	Start(handler http.Handler)
}

type ReplicatedFileLeader struct {
	log         logging.Logger
	writer      io.Writer
	httpStarter HttpStarter
	upgrader    websocket.Upgrader
	lenBuffer   []byte

	syncClients sync.RWMutex
	clients     []*websocket.Conn
}

func NewReplicatedFileLeader(writer io.Writer, httpStarter HttpStarter) *ReplicatedFileLeader {
	r := &ReplicatedFileLeader{
		log:         logging.Log("ReplicatedFileLeader"),
		writer:      writer,
		httpStarter: httpStarter,
		lenBuffer:   make([]byte, 4),
	}
	httpStarter.Start(http.HandlerFunc(r.handleClient))

	return r
}

func (r *ReplicatedFileLeader) Write(data []byte) (int, error) {
	n, err := r.writer.Write(data)
	if err != nil {
		r.log.Panic("Unable to write data", err)
	}

	r.writeToClients(data)

	return n, err
}

func (r *ReplicatedFileLeader) writeToClients(data []byte) {
	r.syncClients.RLock()
	defer r.syncClients.RUnlock()

	encodedData := r.buildData(data)

	for _, client := range r.clients {
		client.WriteMessage(websocket.BinaryMessage, encodedData)
	}
}

func (r *ReplicatedFileLeader) buildData(data []byte) []byte {
	binary.LittleEndian.PutUint32(r.lenBuffer, uint32(len(data)))
	return append(r.lenBuffer, data...)
}

func (r *ReplicatedFileLeader) handleClient(writer http.ResponseWriter, req *http.Request) {
	conn, err := r.upgrader.Upgrade(writer, req, nil)
	if err != nil {
		r.log.Error("Failed to upgrade websocket", err)
		return
	}

	r.syncClients.Lock()
	defer r.syncClients.Unlock()
	r.clients = append(r.clients, conn)
}
