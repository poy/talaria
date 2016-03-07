package client_test

import (
	"net"
	"net/http"
	"time"

	"github.com/apoydence/talaria/pb/messages"
	"github.com/gorilla/websocket"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type mockServer struct {
	serverCh          chan []byte
	connEstablishedCh chan bool
	connDoneCh        chan bool
	pingCh            chan bool
	clientCh          chan *messages.Client
	reqCh             chan *http.Request
	upgrader          websocket.Upgrader
}

func newMockServer() *mockServer {
	return &mockServer{
		reqCh:             make(chan *http.Request, 100),
		serverCh:          make(chan []byte, 100),
		clientCh:          make(chan *messages.Client, 100),
		connEstablishedCh: make(chan bool, 100),
		pingCh:            make(chan bool, 100),
		connDoneCh:        make(chan bool, 100),
	}
}

func (m *mockServer) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	defer GinkgoRecover()
	defer func() {
		m.connDoneCh <- true
	}()
	m.reqCh <- req
	conn, err := m.upgrader.Upgrade(writer, req, nil)
	conn.SetPingHandler(m.pingHandler(conn))
	Expect(err).ToNot(HaveOccurred())
	m.connEstablishedCh <- true

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

func (m *mockServer) pingHandler(c *websocket.Conn) func(string) error {
	return func(message string) error {
		m.pingCh <- true
		err := c.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(time.Second))
		if err == websocket.ErrCloseSent {
			return nil
		}

		if e, ok := err.(net.Error); ok && e.Temporary() {
			return nil
		}
		return err
	}
}
