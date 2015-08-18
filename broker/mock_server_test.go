package broker_test

import (
	"net/http"

	"github.com/apoydence/talaria/messages"
	"github.com/gorilla/websocket"

	. "github.com/onsi/gomega"
)

type mockServer struct {
	serverCh chan []byte
	clientCh chan *messages.Client
	upgrader websocket.Upgrader
}

func newMockServer() *mockServer {
	return &mockServer{
		serverCh: make(chan []byte, 100),
		clientCh: make(chan *messages.Client, 100),
	}
}

func (m *mockServer) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	conn, err := m.upgrader.Upgrade(writer, req, nil)
	Expect(err).ToNot(HaveOccurred())

	clientMsg := readMessage(conn)
	m.clientCh <- clientMsg

	serverMsg := <-m.serverCh

	err = conn.WriteMessage(websocket.BinaryMessage, serverMsg)
	Expect(err).ToNot(HaveOccurred())
}

func readMessage(conn *websocket.Conn) *messages.Client {
	_, data, err := conn.ReadMessage()
	Expect(err).ToNot(HaveOccurred())

	var msg messages.Client
	err = msg.Unmarshal(data)
	Expect(err).ToNot(HaveOccurred())
	return &msg
}
