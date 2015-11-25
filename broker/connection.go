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
	conn      *websocket.Conn
	messageId uint64
	writeCh   chan clientMsgInfo
	errCh     chan error

	lock      sync.Mutex
	clientMap map[uint64]chan<- *messages.Server
}

type clientMsgInfo struct {
	msg    *messages.Client
	respCh chan<- *messages.Server
}

func NewConnection(URL string) (*Connection, error) {
	log := logging.Log("Connection")
	conn, _, err := websocket.DefaultDialer.Dial(URL, nil)
	if err != nil {
		return nil, err
	}

	c := &Connection{
		log:       log,
		conn:      conn,
		errCh:     make(chan error, 100),
		writeCh:   make(chan clientMsgInfo, 100),
		clientMap: make(map[uint64]chan<- *messages.Server),
	}

	go c.readCore()
	go c.writeCore()

	return c, nil
}

func (c *Connection) FetchFile(fileId uint64, name string) *FetchFileError {
	respCh := c.writeFetchFile(c.nextMsgId(), fileId, name)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		return NewFetchFileError(serverMsg.Error.GetMessage(), "")
	}

	if serverMsg.GetMessageType() != messages.Server_FileLocation {
		return NewFetchFileError(fmt.Sprintf("Unexpected MessageType: %v", serverMsg.GetMessageType()), "")
	}

	if !serverMsg.FileLocation.GetLocal() {
		return NewFetchFileError("Redirect", serverMsg.FileLocation.GetUri())
	}

	return nil
}

func (c *Connection) WriteToFile(fileId uint64, data []byte) (int64, error) {
	respCh := c.writeWriteToFile(c.nextMsgId(), fileId, data)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		return 0, fmt.Errorf(serverMsg.Error.GetMessage())
	}

	if serverMsg.GetMessageType() != messages.Server_FileOffset {
		return 0, fmt.Errorf("Unexpected MessageType: %v", serverMsg.GetMessageType())
	}

	return serverMsg.FileOffset.GetOffset(), nil
}

func (c *Connection) ReadFromFile(fileId uint64) ([]byte, error) {
	respCh := c.writeReadFromFile(c.nextMsgId(), fileId)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		return nil, fmt.Errorf(serverMsg.Error.GetMessage())
	}

	if serverMsg.GetMessageType() != messages.Server_ReadData {
		return nil, fmt.Errorf("Unexpected MessageType: %v", serverMsg.GetMessageType())
	}

	return serverMsg.ReadData.GetData(), nil
}

func (c *Connection) InitWriteIndex(fileId uint64, index int64, data []byte) (int64, error) {
	respCh := c.writeInitWriteIndex(c.nextMsgId(), fileId, index, data)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		return 0, fmt.Errorf(serverMsg.Error.GetMessage())
	}

	if serverMsg.GetMessageType() != messages.Server_FileOffset {
		return 0, fmt.Errorf("Unexpected MessageType: %v", serverMsg.GetMessageType())
	}

	return serverMsg.FileOffset.GetOffset(), nil
}

func (c *Connection) Errored() error {
	return nil
}

func (c *Connection) Close() {
	c.conn.Close()
	close(c.writeCh)
}

func (c *Connection) nextMsgId() uint64 {
	return atomic.AddUint64(&c.messageId, 1)
}

func (c *Connection) writeError(err error) {
	select {
	case c.errCh <- err:
	default:
		c.log.Panic("Errors are not being read fast enough", err)
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
			c.writeError(err)
			return
		}

		c.lock.Lock()
		respCh, ok := c.clientMap[msg.GetMessageId()]
		if !ok {
			c.log.Error("Reading client message", fmt.Errorf("Unexpected message ID: %d", msg.GetMessageId()))
			c.lock.Unlock()
			continue
		}
		delete(c.clientMap, msg.GetMessageId())
		c.lock.Unlock()

		respCh <- msg
	}
}

func (c *Connection) writeMessage(msg *messages.Client) {
	data, err := msg.Marshal()
	if err != nil {
		c.log.Panic("Unable to marshal message", err)
	}

	if err = c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		c.writeError(err)
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
