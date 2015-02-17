package restful

import (
	"encoding/json"
	"github.com/apoydence/talaria"
	"github.com/gorilla/pat"
	"github.com/gorilla/websocket"
	"net/http"
	"path"
	"strings"
	"sync"
)

var once sync.Once

type QueueHolder interface {
	AddQueue(queueName string, size talaria.BufferSize) error
	Fetch(queueName string) talaria.Queue
	RemoveQueue(queueName string)
	ListQueues() []talaria.QueueListing
}

type RestServer struct {
	queueHolder QueueHolder
	addr        string
	router      *pat.Router
	jsonDecoder json.Decoder
	wsUpgrader  websocket.Upgrader
}

func StartNewRestServer(queueHolder QueueHolder, addr string) (*RestServer, <-chan error) {
	server := &RestServer{
		queueHolder: queueHolder,
		addr:        addr,
		router:      pat.New(),
		wsUpgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}
	return server, server.start()
}

func (rs *RestServer) start() <-chan error {
	errChan := make(chan error)
	once.Do(func() {
		rs.router.Get("/queues", rs.handleFetchQueue)
		rs.router.Get("/connect", rs.handleConnect)
		rs.router.Post("/queues", rs.handleAddQueue)
		rs.router.Delete("/queues", rs.handleRemove)
		http.Handle("/", rs.router)

		go func() {
			errChan <- http.ListenAndServe(rs.addr, nil)
		}()
	})
	return errChan
}

func (rs *RestServer) handleConnect(resp http.ResponseWriter, req *http.Request) {
	cd := newConnectToQueueData(req.Header)
	var rq, wq talaria.Queue
	if rq = rs.queueHolder.Fetch(cd.ReadQueue); rq == nil && len(cd.ReadQueue) > 0 {
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	if wq = rs.queueHolder.Fetch(cd.WriteQueue); wq == nil && len(cd.WriteQueue) > 0 {
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	conn, err := rs.wsUpgrader.Upgrade(resp, req, nil)
	if err == nil {
		if rq != nil {
			rs.handleQueueReadData(rq, conn)
		}

		if wq != nil {
			rs.handleQueueWriteData(wq, conn)
		} else {
			go infiniteRead(conn)
		}
	}
}

func (rs *RestServer) handleFetchQueue(resp http.ResponseWriter, req *http.Request) {
	parts := fetchUrlParts(req.URL.Path)
	if len(parts) == 1 {
		rs.handleMultiFetchQueue(resp, req)
		return
	} else if len(parts) == 2 {
		rs.handleSingleFetchQueue(resp, parts[1])
		return
	}
	resp.WriteHeader(http.StatusNotFound)
}

func (rs *RestServer) handleQueueReadData(queue talaria.Queue, conn *websocket.Conn) {
	go writeFromQueue(conn, queue)
}

func (rs *RestServer) handleQueueWriteData(queue talaria.Queue, conn *websocket.Conn) {
	go func() {
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil || !queue.Write(data) {
				break
			}
		}
	}()
}

func writeFromQueue(conn *websocket.Conn, queue talaria.Queue) {
	defer conn.Close()
	for {
		data := queue.Read()
		if data == nil {
			break
		}

		err := conn.WriteMessage(websocket.BinaryMessage, data)
		if err != nil {
			break
		}
	}
}

func infiniteRead(conn *websocket.Conn) {
	for {
		if _, _, err := conn.NextReader(); err != nil {
			conn.Close()
			break
		}
	}
}

func (rs *RestServer) handleMultiFetchQueue(resp http.ResponseWriter, req *http.Request) {
	for _, q := range rs.queueHolder.ListQueues() {
		data, err := json.Marshal(NewQueueData(q.Name, q.Q.BufferSize()))
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}
		resp.Write(data)
	}
}

func (rs *RestServer) handleSingleFetchQueue(resp http.ResponseWriter, name string) {
	queue := rs.queueHolder.Fetch(name)

	if queue == nil {
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	var err error
	var data []byte
	if data, err = json.Marshal(NewQueueData(name, queue.BufferSize())); err == nil {
		_, err = resp.Write(data)
	} else {
		resp.WriteHeader(http.StatusInternalServerError)
	}
}

func (rs *RestServer) handleAddQueue(resp http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	data, err := ParseQueueData(req.Form)
	if err == nil {
		err = rs.queueHolder.AddQueue(data.QueueName, data.Buffer)
		resp.WriteHeader(fetchStatusCode(err))
		return
	}

	resp.WriteHeader(http.StatusBadRequest)
}

func (rs *RestServer) handleRemove(resp http.ResponseWriter, req *http.Request) {
	base, name := path.Split(req.URL.Path)
	if base != "/queues/" || len(name) == 0 {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	rs.queueHolder.RemoveQueue(name)
	resp.WriteHeader(http.StatusOK)
}

func fetchStatusCode(err error) int {
	if err == nil {
		return http.StatusOK
	} else {
		return http.StatusBadRequest
	}
}

func fetchUrlParts(url string) []string {
	result := make([]string, 0)
	parts := strings.Split(url, "/")
	for _, p := range parts {
		if len(p) > 0 {
			result = append(result, p)
		}
	}

	return result
}
