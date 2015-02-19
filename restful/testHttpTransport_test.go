package restful_test

import (
	"net/http"
	"net/http/httptest"
)

type testHttpServer struct {
	server *httptest.Server
}

func NewTestHttpServer() *testHttpServer {
	return &testHttpServer{}
}

func (t *testHttpServer) ListenAndServe() error {
	return nil
}

func (t *testHttpServer) SetHandler(handler http.Handler) {
	if t.server != nil {
		panic("SetHandler should only be called once")
	}
	t.server = httptest.NewServer(handler)
}

func (t *testHttpServer) SetAddr(addr string) {
}

func (t *testHttpServer) Close() {
	if t != nil && t.server != nil {
		t.server.Close()
	}
}

func (t *testHttpServer) Endpoint() string {
	if t != nil && t.server != nil {
		return t.server.URL
	}
	return ""
}
