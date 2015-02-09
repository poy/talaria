package restful

import (
	"github.com/apoydence/talaria"
	"net/http"
	"strconv"
)

type QueueHolder interface {
	Fetch(queueName string, size talaria.BufferSize) (talaria.Queue, error)
}

type RestServer struct {
	queueHolder QueueHolder
	addr        string
}

func StartNewRestServer(queueHolder QueueHolder, addr string) (*RestServer, <-chan error) {
	server := &RestServer{
		queueHolder: queueHolder,
		addr:        addr,
	}
	return server, server.start()
}

func (rs *RestServer) start() <-chan error {
	http.HandleFunc("/fetch", rs.handleFetch)

	errChan := make(chan error)
	go func() {
		errChan <- http.ListenAndServe(rs.addr, nil)
	}()
	return errChan
}

func (rs *RestServer) handleFetch(resp http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		err := req.ParseForm()
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			return
		}

		if name, bufferSize, ok := fetchParameters(req.PostForm); ok {
			_, err := rs.queueHolder.Fetch(name, bufferSize)
			resp.WriteHeader(fetchStatusCode(err))
			return
		}
	}
	resp.WriteHeader(http.StatusNotFound)
}

func fetchStatusCode(err error) int {
	if err == nil {
		return http.StatusOK
	} else {
		return http.StatusBadRequest
	}
}

func fetchParameters(form map[string][]string) (string, talaria.BufferSize, bool) {
	var values []string
	var ok bool
	if values, ok = form["name"]; !ok || len(values) != 1 {
		return "", -1, false
	}
	name := values[0]

	if values, ok = form["bufferSize"]; !ok || len(values) != 1 {
		return "", -1, false
	}
	bufferSize, err := strconv.Atoi(values[0])
	if err != nil {
		return "", -1, false
	}

	return name, talaria.BufferSize(bufferSize), true
}
