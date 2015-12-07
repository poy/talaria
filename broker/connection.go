package broker

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/pb/messages"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
)

type Connection struct {
	log       logging.Logger
	URL       string
	conn      *websocket.Conn
	messageId uint64
	writeCh   chan clientMsgInfo

	lock      sync.Mutex
	clientMap map[uint64]chan<- *messages.Server
}

type clientMsgInfo struct {
	msg    *messages.Client
	respCh chan<- *messages.Server
}

func NewConnection(URL string) (*Connection, error) {
	log := logging.Log("Connection")

	log.Debug("Connecting to %s", URL)
	conn, _, err := websocket.DefaultDialer.Dial(URL, nil)
	if err != nil {
		return nil, err
	}

	c := &Connection{
		log:       log,
		URL:       URL,
		conn:      conn,
		writeCh:   make(chan clientMsgInfo, 100),
		clientMap: make(map[uint64]chan<- *messages.Server),
	}

	go c.readCore()
	go c.writeCore()

	return c, nil
}

func (c *Connection) FetchFile(fileId uint64, name string) *ConnectionError {
	respCh := c.writeFetchFile(c.nextMsgId(), fileId, name)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		return NewConnectionError(serverMsg.Error.GetMessage(), "", serverMsg.Error.GetConnection())
	}

	if serverMsg.GetMessageType() != messages.Server_FileLocation {
		return NewConnectionError(fmt.Sprintf("Expected MessageType: %v. Received %v", messages.Server_FileLocation, serverMsg.GetMessageType()), "", false)
	}

	if !serverMsg.FileLocation.GetLocal() {
		return NewConnectionError("Redirect", serverMsg.FileLocation.GetUri(), false)
	}

	return nil
}

func (c *Connection) WriteToFile(fileId uint64, data []byte) (int64, *ConnectionError) {
	respCh := c.writeWriteToFile(c.nextMsgId(), fileId, data)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		return 0, NewConnectionError(serverMsg.Error.GetMessage(), "", serverMsg.Error.GetConnection())
	}

	if serverMsg.GetMessageType() != messages.Server_FileOffset {
		return 0, NewConnectionError(fmt.Sprintf("Unexpected MessageType: %v", serverMsg.GetMessageType()), "", false)
	}

	return serverMsg.FileOffset.GetOffset(), nil
}

func (c *Connection) ReadFromFile(fileId uint64) ([]byte, int64, *ConnectionError) {
	respCh := c.writeReadFromFile(c.nextMsgId(), fileId)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		return nil, 0, NewConnectionError(serverMsg.Error.GetMessage(), "", serverMsg.Error.GetConnection())
	}

	if serverMsg.GetMessageType() != messages.Server_ReadData {
		return nil, 0, NewConnectionError(fmt.Sprintf("Unexpected MessageType: %v", serverMsg.GetMessageType()), "", false)
	}

	data := serverMsg.ReadData.GetData()
	index := serverMsg.ReadData.GetOffset()
	return data, index, nil
}

func (c *Connection) InitWriteIndex(fileId uint64, index int64, data []byte) (int64, *ConnectionError) {
	respCh := c.writeInitWriteIndex(c.nextMsgId(), fileId, index, data)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		return 0, NewConnectionError(serverMsg.Error.GetMessage(), "", serverMsg.Error.GetConnection())
	}

	if serverMsg.GetMessageType() != messages.Server_FileOffset {
		return 0, NewConnectionError(fmt.Sprintf("Unexpected MessageType: %v", serverMsg.GetMessageType()), "", false)
	}

	return serverMsg.FileOffset.GetOffset(), nil
}

func (c *Connection) Close() {
	c.conn.Close()
	close(c.writeCh)
}

func (c *Connection) nextMsgId() uint64 {
	return atomic.AddUint64(&c.messageId, 1)
}

func (c *Connection) submitWebsocketError(err error, messageId uint64) {
	c.submitServerResponse(&messages.Server{
		MessageType: messages.Server_Error.Enum(),
		MessageId:   proto.Uint64(messageId),
		Error: &messages.Error{
			Message:    proto.String(err.Error()),
			Connection: proto.Bool(true),
		},
	})
}

func (c *Connection) alertWaiting(err error) {
	var msgIds []uint64

	c.lock.Lock()
	for msgId, _ := range c.clientMap {
		msgIds = append(msgIds, msgId)
	}
	c.lock.Unlock()

	for _, msgId := range msgIds {
		c.submitWebsocketError(err, msgId)
	}
}

func (c *Connection) readMessage() (*messages.Server, error) {
	_, data, err := c.conn.ReadMessage()
	if err != nil {
		return nil, err
	}

	server := &messages.Server{}
	err = server.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (c *Connection) writeCore() {
	for msg := range c.writeCh {
		c.lock.Lock()
		c.clientMap[msg.msg.GetMessageId()] = msg.respCh
		c.lock.Unlock()

		c.writeMessage(msg.msg)
	}
}

func (c *Connection) readCore() {
	for {
		msg, err := c.readMessage()
		if err != nil {
			c.log.Errorf("Failed to read from %s: %v", c.URL, err)
			c.conn.Close()
			c.alertWaiting(err)
			return
		}

		c.submitServerResponse(msg)
	}
}

func (c *Connection) submitServerResponse(msg *messages.Server) {
	c.lock.Lock()
	respCh, ok := c.clientMap[msg.GetMessageId()]
	if !ok {
		c.log.Errorf("Reading client message: Unexpected message ID: %d", msg.GetMessageId())
		c.lock.Unlock()
		return
	}

	delete(c.clientMap, msg.GetMessageId())
	c.lock.Unlock()

	respCh <- msg
}

func (c *Connection) writeMessage(msg *messages.Client) {
	data, err := msg.Marshal()
	if err != nil {
		c.log.Panic("Unable to marshal message", err)
	}

	if err = c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		c.log.Errorf("Failed to write to%s: %v", c.URL, err)
		c.submitWebsocketError(err, msg.GetMessageId())
		return
	}
}

func (c *Connection) writeFetchFile(msgId, fileId uint64, name string) <-chan *messages.Server {
	messageType := messages.Client_FetchFile
	msg := &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		FetchFile: &messages.FetchFile{
			Name:   proto.String(name),
			FileId: proto.Uint64(fileId),
		},
	}

	respCh := make(chan *messages.Server, 1)
	c.writeCh <- clientMsgInfo{
		respCh: respCh,
		msg:    msg,
	}

	return respCh
}

func (c *Connection) writeWriteToFile(msgId, fileId uint64, data []byte) <-chan *messages.Server {
	messageType := messages.Client_WriteToFile
	msg := &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		WriteToFile: &messages.WriteToFile{
			FileId: proto.Uint64(fileId),
			Data:   data,
		},
	}

	respCh := make(chan *messages.Server, 1)
	c.writeCh <- clientMsgInfo{
		respCh: respCh,
		msg:    msg,
	}

	return respCh
}

func (c *Connection) writeReadFromFile(msgId, fileId uint64) <-chan *messages.Server {
	messageType := messages.Client_ReadFromFile
	msg := &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		ReadFromFile: &messages.ReadFromFile{
			FileId: proto.Uint64(fileId),
		},
	}

	respCh := make(chan *messages.Server, 1)
	c.writeCh <- clientMsgInfo{
		respCh: respCh,
		msg:    msg,
	}

	return respCh
}

func (c *Connection) writeInitWriteIndex(msgId, fileId uint64, index int64, data []byte) <-chan *messages.Server {
	messageType := messages.Client_InitWriteIndex
	msg := &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		InitWriteIndex: &messages.InitWriteIndex{
			FileId: proto.Uint64(fileId),
			Index:  proto.Int64(index),
			Data:   data,
		},
	}

	respCh := make(chan *messages.Server, 1)
	c.writeCh <- clientMsgInfo{
		respCh: respCh,
		msg:    msg,
	}

	return respCh
}
