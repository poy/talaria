package restful

import (
	"encoding/json"
	"github.com/apoydence/talaria"
	"github.com/gorilla/pat"
	"io"
	"net/http"
	"path"
	"sync"
)

var once sync.Once

type QueueHolder interface {
	AddQueue(queueName string, size talaria.BufferSize) error
	Fetch(queueName string) talaria.Queue
	RemoveQueue(queueName string)
}

type RestServer struct {
	queueHolder QueueHolder
	addr        string
	router      *pat.Router
	jsonDecoder json.Decoder
}

type QueueData struct {
	QueueName string             `json:"queueName"`
	Buffer    talaria.BufferSize `json:"bufferSize"`
}

func StartNewRestServer(queueHolder QueueHolder, addr string) (*RestServer, <-chan error) {
	server := &RestServer{
		queueHolder: queueHolder,
		addr:        addr,
		router:      pat.New(),
	}
	return server, server.start()
}

func (rs *RestServer) start() <-chan error {
	errChan := make(chan error)
	once.Do(func() {
		rs.router.Post("/queues", rs.handleAddQueue)
		rs.router.Delete("/queues", rs.handleRemove)
		http.Handle("/", rs.router)

		go func() {
			errChan <- http.ListenAndServe(rs.addr, nil)
		}()
	})
	return errChan
}

func (rs *RestServer) handleAddQueue(resp http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	dec := json.NewDecoder(req.Body)
	var data QueueData
	if err = dec.Decode(&data); err == io.EOF || err == nil {
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
