package restful

import "net/http"

type defaultHttpServer struct {
	httpServer *http.Server
	handler    http.Handler
}

func NewDefaultHttpServer() *defaultHttpServer {
	return &defaultHttpServer{
		httpServer: &http.Server{},
	}
}

func (srv *defaultHttpServer) ListenAndServe() error {
	return srv.httpServer.ListenAndServe()
}

func (srv *defaultHttpServer) SetAddr(addr string) {
	srv.httpServer.Addr = addr
}

func (srv *defaultHttpServer) SetHandler(handler http.Handler) {
	srv.handler = handler
}
