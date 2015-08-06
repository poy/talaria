package files_test

import (
	"net/http"
	"net/http/httptest"
)

type mockHttpStarter struct {
	server *httptest.Server
}

func newMockHttpStarter() *mockHttpStarter {
	return &mockHttpStarter{}
}

func (m *mockHttpStarter) Start(handler http.Handler) {
	m.server = httptest.NewServer(handler)
}

func (m *mockHttpStarter) stop() {
	if m.server == nil {
		return
	}
	m.server.Close()
}
