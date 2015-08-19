package files

import (
	"encoding/binary"
	"io"

	"github.com/apoydence/talaria/logging"
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
			continue
		}

		for i := uint32(0); i < uint32(len(data)); {
			length := binary.LittleEndian.Uint32(data[i:])
			r.writer.Write(data[i+4 : i+4+length])
			i += length + 4
		}
	}
}
