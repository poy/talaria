package restful

import (
	"encoding/json"
	"github.com/apoydence/talaria"
	"github.com/gorilla/pat"
	"github.com/gorilla/websocket"
	"net/http"
	"path"
	"strings"
)

type QueueHolder interface {
	AddQueue(queueName string, size talaria.BufferSize) error
	RemoveQueue(queueName string)
	ListQueues() chan QueueData
}

type RemoteQueueHolder interface {
	QueueHolder
	Fetch(queueName string, blacklist ...string) (talaria.Queue, string, int)
}

type HttpServer interface {
	ListenAndServe() error
	SetHandler(handler http.Handler)
	SetAddr(addr string)
}

type RestServer struct {
	queueHolder    RemoteQueueHolder
	neighborHolder NeighborHolder
	HttpClient     HttpClient
	HttpServer     HttpServer
	addr           string
	router         *pat.Router
	jsonDecoder    json.Decoder
	wsUpgrader     websocket.Upgrader
}

func NewRestServer(queueHolder RemoteQueueHolder, ns NeighborHolder, addr string) *RestServer {
	server := &RestServer{
		queueHolder:    queueHolder,
		neighborHolder: ns,
		HttpClient:     NewDefaultHttpClient(),
		HttpServer:     NewDefaultHttpServer(),
		addr:           addr,
		router:         pat.New(),
		wsUpgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}
	return server
}

func (rs *RestServer) Start() <-chan error {
	errChan := make(chan error)
	rs.router.Get("/queues", rs.handleFetchQueue)
	rs.router.Get("/connect", rs.handleConnect)
	rs.router.Post("/queues", rs.handleAddQueue)
	rs.router.Delete("/queues", rs.handleRemove)
	rs.HttpServer.SetHandler(rs.router)
	rs.HttpServer.SetAddr(rs.addr)

	go func() {
		errChan <- rs.HttpServer.ListenAndServe()
	}()
	return errChan
}

func (rs *RestServer) handleConnect(resp http.ResponseWriter, req *http.Request) {
	cd := newConnectToQueueData(req.Header)
	var rq, wq talaria.Queue
	if rq, _, _ = rs.queueHolder.Fetch(cd.ReadQueue); rq == nil && len(cd.ReadQueue) > 0 {
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	if wq, _, _ = rs.queueHolder.Fetch(cd.WriteQueue); wq == nil && len(cd.WriteQueue) > 0 {
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
	/*
		for q := range rs.queueHolder.ListQueues() {
			data, err := json.Marshal(NewQueueData(q.Name, q.Q.BufferSize(), ""))
			if err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				return
			}
			resp.Write(data)
		}
	*/
	panic("Not implemented")
}

func (rs *RestServer) handleSingleFetchQueue(resp http.ResponseWriter, name string) {
	queueData, _, code, err := rs.fetchQueue(name)

	if code != http.StatusOK {
		resp.WriteHeader(code)
		return
	} else if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	var data []byte
	if data, err = json.Marshal(queueData); err == nil {
		_, err = resp.Write(data)
	} else {
		resp.WriteHeader(http.StatusInternalServerError)
	}
}

func (rs *RestServer) fetchQueue(name string) (QueueData, bool, int, error) {
	queue, _, _ := rs.queueHolder.Fetch(name)
	if queue != nil {
		return NewQueueData(name, queue.BufferSize(), rs.addr), true, http.StatusOK, nil
	}

	ns := rs.neighborHolder.GetNeighbors()
	for _, n := range ns {
		resp, err := rs.HttpClient.Get(n.Endpoint+"/queues/"+name, nil)
		if err != nil {
			return QueueData{}, false, resp.StatusCode, err
		} else if resp.StatusCode != http.StatusOK {
			continue
		}

		dec := json.NewDecoder(resp.Body)
		var data QueueData
		err = dec.Decode(&data)
		return data, false, resp.StatusCode, err
	}

	return QueueData{}, false, http.StatusNotFound, nil
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

	queue, local, code, err := rs.fetchQueue(name)
	if local {
		rs.queueHolder.RemoveQueue(name)
		resp.WriteHeader(http.StatusOK)
		return
	} else if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	} else if code != http.StatusOK {
		resp.WriteHeader(code)
		return
	}

	resp2, err := rs.HttpClient.Delete(queue.Endpoint+"/queues/"+name, nil)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	} else if resp2.StatusCode != http.StatusOK {
		resp.WriteHeader(code)
	}

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
