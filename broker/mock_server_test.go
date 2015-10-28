package broker_test

import (
	"net/http"

	"github.com/apoydence/talaria/pb/messages"
	"github.com/gorilla/websocket"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type mockServer struct {
	serverCh          chan []byte
	connEstablishedCh chan struct{}
	clientCh          chan *messages.Client
	reqCh             chan *http.Request
	upgrader          websocket.Upgrader
}

func newMockServer() *mockServer {
	return &mockServer{
		reqCh:             make(chan *http.Request, 100),
		serverCh:          make(chan []byte, 100),
		clientCh:          make(chan *messages.Client, 100),
		connEstablishedCh: make(chan struct{}),
	}
}

func (m *mockServer) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	defer GinkgoRecover()
	m.reqCh <- req
	conn, err := m.upgrader.Upgrade(writer, req, nil)
	Expect(err).ToNot(HaveOccurred())
	close(m.connEstablishedCh)

	for {
		clientMsg := m.readMessage(conn)
		if clientMsg == nil {
			return
		}
		m.clientCh <- clientMsg

		serverMsg := <-m.serverCh
		if serverMsg == nil {
			return
		}

		err = conn.WriteMessage(websocket.BinaryMessage, serverMsg)
		Expect(err).ToNot(HaveOccurred())
	}
}

func (m *mockServer) readMessage(conn *websocket.Conn) *messages.Client {
	_, data, err := conn.ReadMessage()
	if err != nil {
		return nil
	}

	var msg messages.Client
	err = msg.Unmarshal(data)
	Expect(err).ToNot(HaveOccurred())
	return &msg
}
