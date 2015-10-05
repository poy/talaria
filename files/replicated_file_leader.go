package files

import (
	"io"
	"net/http"
	"sync"

	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/messages"
	"github.com/gorilla/websocket"
)

type HttpStarter interface {
	Start(handler http.Handler)
}

type ReadSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

type clientInfo struct {
	conn   *websocket.Conn
	respCh <-chan *messages.Server
}

type ReplicatedFileLeader struct {
	log         logging.Logger
	writer      io.Writer
	httpStarter HttpStarter
	upgrader    websocket.Upgrader
	lenBuffer   []byte
	preData     ReadSeekCloser

	syncClients sync.RWMutex
	clients     []*clientInfo
}

func NewReplicatedFileLeader(writer io.Writer, preData ReadSeekCloser, httpStarter HttpStarter) *ReplicatedFileLeader {
	r := &ReplicatedFileLeader{
		log:         logging.Log("ReplicatedFileLeader"),
		writer:      writer,
		httpStarter: httpStarter,
		lenBuffer:   make([]byte, 4),
		preData:     preData,
	}
	httpStarter.Start(http.HandlerFunc(r.handleClient))

	return r
}

func (r *ReplicatedFileLeader) Write(data []byte) (int, error) {
	r.writeToClients(data)

	n, err := r.writer.Write(data)
	if err != nil {
		r.log.Panic("Unable to write data", err)
	}

	return n, err
}

func (r *ReplicatedFileLeader) writeToClients(data []byte) {
	r.syncClients.RLock()
	defer r.syncClients.RUnlock()

	for _, client := range r.clients {
		client.conn.WriteMessage(websocket.BinaryMessage, data)
		<-client.respCh
	}
}

func (r *ReplicatedFileLeader) handleClient(writer http.ResponseWriter, req *http.Request) {
	conn, err := r.upgrader.Upgrade(writer, req, nil)
	if err != nil {
		r.log.Error("Failed to upgrade websocket", err)
		return
	}

	respCh := make(chan *messages.Server, 10)
	go r.readFromClient(conn, respCh)

	r.syncClients.Lock()
	defer r.syncClients.Unlock()
	r.writePreData(conn, respCh)

	r.clients = append(r.clients, &clientInfo{
		conn:   conn,
		respCh: respCh,
	})
}

func (r *ReplicatedFileLeader) writePreData(conn *websocket.Conn, respCh <-chan *messages.Server) {
	r.preData.Seek(0, 0)
	buffer := make([]byte, 1024)

	for {
		n, err := r.preData.Read(buffer)
		if err == io.EOF && n == 0 {
			return
		}

		if err != nil {
			r.log.Panic("Unable to read from pre-data", err)
		}

		conn.WriteMessage(websocket.BinaryMessage, buffer[:n])
		<-respCh
	}
}

func (r *ReplicatedFileLeader) readFromClient(conn *websocket.Conn, readCh chan<- *messages.Server) {
	for {
		_, msgData, err := conn.ReadMessage()
		if err != nil {
			return
		}

		msg := &messages.Server{}
		if err = msg.Unmarshal(msgData); err != nil {
			r.log.Error("Failed to unmarshal response", err)
			return
		}

		readCh <- msg
	}
}
