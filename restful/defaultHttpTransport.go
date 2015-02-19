package restful

import "net/http"

type defaultHttpClient struct {
	httpClient *http.Client
}

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

func NewDefaultHttpClient() *defaultHttpClient {
	return &defaultHttpClient{
		httpClient: &http.Client{},
	}
}

func (client *defaultHttpClient) Get(url string) (resp *http.Response, err error) {
	return client.httpClient.Get(url)
}

func (client *defaultHttpClient) Delete(url string) (resp *http.Response, err error) {
	req, _ := http.NewRequest("DELETE", url, nil)
	return client.httpClient.Do(req)
}
