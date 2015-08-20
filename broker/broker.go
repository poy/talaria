package broker

import (
	"fmt"
	"net/http"
	"time"

	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/messages"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
)

const (
	FetchFile uint32 = iota
	WriteToFile
)

type Controller interface {
	FetchFile(name string) (uint64, error)
	WriteToFile(id uint64, data []byte) (int64, error)
	ReadFromFile(id uint64) ([]byte, error)
}

func StartBrokerServer(dataDir string, brokerPort int, segmentLength, numSegments uint64) {
	log := logging.Log("BrokerServer")
	provider := NewFileProvider(dataDir, segmentLength, numSegments, time.Second)
	controller := NewFileController(provider)
	broker := NewBroker(controller)

	log.Info("Starting broker on port %d", brokerPort)
	http.ListenAndServe(fmt.Sprintf(":%d", brokerPort), broker)
}

type Broker struct {
	log        logging.Logger
	upgrader   websocket.Upgrader
	controller Controller
}

func NewBroker(controller Controller) *Broker {
	return &Broker{
		log:        logging.Log("Broker"),
		controller: controller,
	}
}

func (b *Broker) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	conn, err := b.upgrader.Upgrade(writer, req, nil)
	if err != nil {
		b.log.Error("Unable to upgrade websocket", err)
		return
	}

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			b.log.Error("Unable to read from websocket", err)
			return
		}

		message := &messages.Client{}
		err = message.Unmarshal(data)
		if err != nil {
			b.log.Error("Unable to read message", err)
			return
		}

		switch message.GetMessageType() {
		case messages.Client_FetchFile:
			b.fetchFile(message, conn)
		case messages.Client_WriteToFile:
			b.writeToFile(message, conn)
		case messages.Client_ReadFromFile:
			b.readFromFile(message, conn)
		}
	}
}

func (b *Broker) fetchFile(message *messages.Client, conn *websocket.Conn) {
	fileId, err := b.controller.FetchFile(message.GetFetchFile().GetName())
	if err != nil {
		b.writeError(err.Error(), message, conn)
		return
	}

	b.writeFileLocation(fileId, message, conn)
}

func (b *Broker) writeToFile(message *messages.Client, conn *websocket.Conn) {
	offset, err := b.controller.WriteToFile(message.WriteToFile.GetFileId(), message.WriteToFile.GetData())
	if err != nil {
		b.writeError(err.Error(), message, conn)
		return
	}

	b.writeFileOffset(offset, message, conn)
}

func (b *Broker) readFromFile(message *messages.Client, conn *websocket.Conn) {
	data, err := b.controller.ReadFromFile(message.ReadFromFile.GetFileId())
	if err != nil {
		b.writeError(err.Error(), message, conn)
		return
	}

	b.writeReadData(data, message, conn)
}

func (b *Broker) writeMessage(message *messages.Server, conn *websocket.Conn) {
	data, err := message.Marshal()
	if err != nil {
		b.log.Panic("Unable to marshal error", err)
	}

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	if err != nil {
		b.log.Panic("Unable to write message", err)
	}
}

func (b *Broker) writeError(errStr string, message *messages.Client, conn *websocket.Conn) {
	msgType := messages.Server_Error
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(message.GetMessageId()),
		Error: &messages.Error{
			Message: proto.String(errStr),
		},
	}
	b.writeMessage(server, conn)
}

func (b *Broker) writeFileLocation(fileId uint64, message *messages.Client, conn *websocket.Conn) {
	msgType := messages.Server_FileLocation
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(message.GetMessageId()),
		FileLocation: &messages.FileLocation{
			Local:  proto.Bool(true),
			FileId: proto.Uint64(fileId),
		},
	}
	b.writeMessage(server, conn)
}

func (b *Broker) writeFileOffset(offset int64, message *messages.Client, conn *websocket.Conn) {
	msgType := messages.Server_FileOffset
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(message.GetMessageId()),
		FileOffset: &messages.FileOffset{
			Offset: proto.Int64(offset),
		},
	}
	b.writeMessage(server, conn)
}

func (b *Broker) writeReadData(data []byte, message *messages.Client, conn *websocket.Conn) {
	msgType := messages.Server_ReadData
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(message.GetMessageId()),
		ReadData: &messages.ReadData{
			Data: data,
		},
	}
	b.writeMessage(server, conn)
}
