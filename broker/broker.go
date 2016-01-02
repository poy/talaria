package broker

import (
	"fmt"
	"io"
	"net/http"

	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/pb/messages"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
)

const (
	OuterEndpoint = "/"
	InnerEndpoint = "/inner"
)

type Controller interface {
	FetchFile(id uint64, name string) *ConnectionError
	WriteToFile(id uint64, data []byte) (int64, error)
	ReadFromFile(id uint64, callback func([]byte, int64, error))
	SeekIndex(id, index uint64, callback func(error))
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

	conWriter := newConcurrentWriter(conn)

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
			b.fetchFile(controller, message, conWriter)
		case messages.Client_WriteToFile:
			b.writeToFile(controller, message, conWriter)
		case messages.Client_ReadFromFile:
			b.readFromFile(controller, message, conWriter)
		case messages.Client_SeekIndex:
			b.seekIndex(controller, message, conWriter)
		}
	}
}

func (b *Broker) fetchFile(controller Controller, message *messages.Client, conn *concurrentWriter) {
	fetchFile := message.GetFetchFile()
	err := controller.FetchFile(fetchFile.GetFileId(), fetchFile.GetName())
	if err != nil && err.Uri == "" {
		b.writeError(err.Error(), message, conn)
		return
	}

	b.writeFileLocation(err, message, conn)
}

func (b *Broker) writeToFile(controller Controller, message *messages.Client, conn *concurrentWriter) {
	offset, err := controller.WriteToFile(message.WriteToFile.GetFileId(), message.WriteToFile.GetData())

	if err != nil {
		b.writeError(err.Error(), message, conn)
		return
	}

	b.writeFileOffset(offset, message, conn)
}

func (b *Broker) readFromFile(controller Controller, message *messages.Client, conn *concurrentWriter) {
	callback := func(data []byte, offset int64, err error) {
		if err != nil {
			b.writeError(err.Error(), message, conn)
			return
		}

		b.writeReadData(data, offset, message, conn)
	}
	controller.ReadFromFile(message.ReadFromFile.GetFileId(), callback)
}

func (b *Broker) seekIndex(controller Controller, message *messages.Client, conn *concurrentWriter) {
	callback := func(err error) {

		if err != nil {
			b.writeError(err.Error(), message, conn)
			return
		}

		b.writeFileOffset(int64(message.SeekIndex.GetIndex()), message, conn)
	}

	controller.SeekIndex(message.SeekIndex.GetFileId(), message.SeekIndex.GetIndex(), callback)
}

func (b *Broker) writeMessage(message *messages.Server, writer io.Writer) {
	data, err := message.Marshal()
	if err != nil {
		b.log.Panic("Unable to marshal error", err)
	}

	_, err = writer.Write(data)
	if err != nil {
		b.log.Panic("Unable to write message", err)
	}
}

func (b *Broker) writeError(errStr string, message *messages.Client, conn *concurrentWriter) {
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

func (b *Broker) writeFileLocation(connectionErr *ConnectionError, message *messages.Client, conn *concurrentWriter) {
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

func (b *Broker) writeFileOffset(offset int64, message *messages.Client, conn *concurrentWriter) {
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

func (b *Broker) writeReadData(data []byte, offset int64, message *messages.Client, conn *concurrentWriter) {
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
