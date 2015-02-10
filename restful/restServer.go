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
	ListQueues() []talaria.QueueListing
}

type RestServer struct {
	queueHolder QueueHolder
	addr        string
	router      *pat.Router
	jsonDecoder json.Decoder
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
		rs.router.Get("/queues", rs.handleFetchQueue)
		rs.router.Post("/queues", rs.handleAddQueue)
		rs.router.Delete("/queues", rs.handleRemove)
		http.Handle("/", rs.router)

		go func() {
			errChan <- http.ListenAndServe(rs.addr, nil)
		}()
	})
	return errChan
}

func (rs *RestServer) handleFetchQueue(resp http.ResponseWriter, req *http.Request) {
	base, name := path.Split(req.URL.Path)
	if base != "/queues/" || len(name) == 0 {
		rs.handleMultiFetchQueue(resp, req)
		return
	}
	rs.handleSingleFetchQueue(resp, name)
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
