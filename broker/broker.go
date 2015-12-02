package broker

import (
	"fmt"
	"net/http"

	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/pb/messages"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
)

const (
	FetchFile uint32 = iota
	WriteToFile
)

const (
	OuterEndpoint = "/"
	InnerEndpoint = "/inner"
)

type Controller interface {
	FetchFile(id uint64, name string) *ConnectionError
	WriteToFile(id uint64, data []byte) (int64, error)
	ReadFromFile(id uint64) ([]byte, int64, error)
	InitWriteIndex(id uint64, index int64, data []byte) (int64, error)
}

type ControllerProvider interface {
	Provide() Controller
}

func StartBrokerServer(brokerPort int, orch Orchestrator, provider IoProvider) {
	log := logging.Log("BrokerServer")
	controllerProvider := newControllerProvider(false, provider, orch)
	broker := NewBroker(controllerProvider)

	controllerProviderInner := newControllerProvider(true, provider, orch)
	brokerInner := NewBroker(controllerProviderInner)

	http.Handle(OuterEndpoint, broker)
	http.Handle(InnerEndpoint, brokerInner)

	log.Info("Starting broker on port %d", brokerPort)
	uri := fmt.Sprintf(":%d", brokerPort)
	if err := http.ListenAndServe(uri, nil); err != nil {
		log.Panicf("Unable to start server %s: %v", uri, err)
	}
}

type Broker struct {
	log                logging.Logger
	upgrader           websocket.Upgrader
	controllerProvider ControllerProvider
}

func NewBroker(controllerProvider ControllerProvider) *Broker {
	return &Broker{
		log:                logging.Log("Broker"),
		controllerProvider: controllerProvider,
	}
}

func (b *Broker) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	controller := b.controllerProvider.Provide()
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
			b.fetchFile(controller, message, conn)
		case messages.Client_WriteToFile:
			b.writeToFile(controller, message, conn)
		case messages.Client_ReadFromFile:
			b.readFromFile(controller, message, conn)
		case messages.Client_InitWriteIndex:
			b.initWriteIndex(controller, message, conn)
		}
	}
}

func (b *Broker) fetchFile(controller Controller, message *messages.Client, conn *websocket.Conn) {
	fetchFile := message.GetFetchFile()
	err := controller.FetchFile(fetchFile.GetFileId(), fetchFile.GetName())
	if err != nil && err.Uri == "" {
		b.writeError(err.Error(), message, conn)
		return
	}

	b.writeFileLocation(err, message, conn)
}

func (b *Broker) writeToFile(controller Controller, message *messages.Client, conn *websocket.Conn) {
	offset, err := controller.WriteToFile(message.WriteToFile.GetFileId(), message.WriteToFile.GetData())

	if err != nil {
		b.writeError(err.Error(), message, conn)
		return
	}

	b.writeFileOffset(offset, message, conn)
}

func (b *Broker) readFromFile(controller Controller, message *messages.Client, conn *websocket.Conn) {
	data, offset, err := controller.ReadFromFile(message.ReadFromFile.GetFileId())
	if err != nil {
		b.writeError(err.Error(), message, conn)
		return
	}

	b.writeReadData(data, offset, message, conn)
}

func (b *Broker) initWriteIndex(controller Controller, message *messages.Client, conn *websocket.Conn) {
	offset, err := controller.InitWriteIndex(message.InitWriteIndex.GetFileId(), message.InitWriteIndex.GetIndex(), message.InitWriteIndex.GetData())

	if err != nil {
		b.writeError(err.Error(), message, conn)
		return
	}

	b.writeFileOffset(offset, message, conn)
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

func (b *Broker) writeFileLocation(connectionErr *ConnectionError, message *messages.Client, conn *websocket.Conn) {
	var uri string
	if connectionErr != nil {
		uri = connectionErr.Uri
	}

	msgType := messages.Server_FileLocation
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(message.GetMessageId()),
		FileLocation: &messages.FileLocation{
			Local: proto.Bool(uri == ""),
			Uri:   proto.String(uri),
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

func (b *Broker) writeReadData(data []byte, offset int64, message *messages.Client, conn *websocket.Conn) {
	msgType := messages.Server_ReadData
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(message.GetMessageId()),
		ReadData: &messages.ReadData{
			Data:   data,
			Offset: proto.Int64(offset),
		},
	}
	b.writeMessage(server, conn)
}
