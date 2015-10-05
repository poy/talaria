package files

import (
	"io"

	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/messages"
	"github.com/gorilla/websocket"
)

type ReplicatedFileClient struct {
	log    logging.Logger
	writer io.Writer
}

func NewReplicatedFileClient(url string, writer io.Writer) *ReplicatedFileClient {
	r := &ReplicatedFileClient{
		log:    logging.Log("ReplicatedFileClient"),
		writer: writer,
	}

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		r.log.Panic("Unable to dial leader", err)
	}

	go r.run(conn)

	return r
}

func (r *ReplicatedFileClient) run(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			r.log.Error("Unable to read from leader", err)
			return
		}

		_, err = r.writer.Write(data)
		if err != nil {
			r.log.Panic("Unable to write to replicated file", err)
		}

		err = conn.WriteMessage(websocket.BinaryMessage, r.buildFileOffset())
		if err != nil {
			r.log.Error("Unable to write to leader", err)
			return
		}
	}
}

func (r *ReplicatedFileClient) buildFileOffset() []byte {
	msg := &messages.Server{
		MessageType: messages.Server_FileOffset.Enum(),
		FileOffset:  &messages.FileOffset{},
	}

	data, err := msg.Marshal()
	if err != nil {
		r.log.Panic("Unable to marshal message", err)
	}

	return data
}
