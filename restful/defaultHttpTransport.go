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
func (client *defaultHttpClient) Get(url string, header http.Header) (*http.Response, error) {
	return client.do(url, "GET", header)
}

func (client *defaultHttpClient) Delete(url string, header http.Header) (resp *http.Response, err error) {
	return client.do(url, "DELETE", header)
}

func (client *defaultHttpClient) do(url, method string, header http.Header) (*http.Response, error) {
	var req *http.Request
	var err error
	req, err = http.NewRequest(method, url, nil)
	if err == nil {
		if header != nil {
			for k, vs := range header {
				for _, v := range vs {
					req.Header.Add(k, v)
				}
			}
		}
		return client.httpClient.Do(req)
	}
	return nil, err
}
