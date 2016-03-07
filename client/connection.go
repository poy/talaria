package client

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apoydence/talaria/common"
	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/pb/messages"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
)

const (
	errored       uint32 = 1
	watchDogReset        = 2 * time.Second
)

type Connection struct {
	log     logging.Logger
	URL     string
	errored uint32

	closeOnce sync.Once
	conn      *websocket.Conn
	messageId uint64
	writeCh   chan clientMsgInfo
	doneCh    chan struct{}
	watchDog  *time.Timer

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

	watchDog := time.NewTimer(watchDogReset)

	conn.SetPongHandler(func(message string) error {
		watchDog.Reset(watchDogReset)
		return nil
	})

	c := &Connection{
		log:       log,
		URL:       URL,
		conn:      conn,
		writeCh:   make(chan clientMsgInfo, 100),
		doneCh:    make(chan struct{}),
		clientMap: make(map[uint64]chan<- *messages.Server),
		watchDog:  watchDog,
	}

	go c.readCore()
	go c.writeCore()
	go c.watchDogCore()

	return c, nil
}

func (c *Connection) FetchFile(fileId uint64, name string, create bool) *common.ConnectionError {
	respCh := c.writeFetchFile(c.nextMsgId(), fileId, name, create)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		c.setErrored()
		return common.NewConnectionError(serverMsg.Error.GetMessage(), "", c.URL, serverMsg.Error.GetConnection())
	}

	if serverMsg.GetMessageType() != messages.Server_FileLocation {
		c.setErrored()
		return common.NewConnectionError(fmt.Sprintf("Expected MessageType: %v. Received %v", messages.Server_FileLocation, serverMsg.GetMessageType()), "", c.URL, false)
	}

	if !serverMsg.FileLocation.GetLocal() {
		return common.NewConnectionError("Redirect", serverMsg.FileLocation.GetUri(), c.URL, false)
	}

	return nil
}

func (c *Connection) WriteToFile(fileId uint64, data []byte) (int64, *common.ConnectionError) {
	respCh := c.writeWriteToFile(c.nextMsgId(), fileId, data)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		c.setErrored()
		return 0, common.NewConnectionError(serverMsg.Error.GetMessage(), "", c.URL, serverMsg.Error.GetConnection())
	}

	if serverMsg.GetMessageType() != messages.Server_FileIndex {
		c.setErrored()
		return 0, common.NewConnectionError(fmt.Sprintf("Expected MessageType: %v. Received %v", messages.Server_FileIndex, serverMsg.GetMessageType()), "", c.URL, false)
	}

	return serverMsg.FileIndex.GetIndex(), nil
}

func (c *Connection) ReadFromFile(fileId uint64) ([]byte, int64, *common.ConnectionError) {
	respCh := c.writeReadFromFile(c.nextMsgId(), fileId)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		c.setErrored()
		return nil, 0, common.NewConnectionError(serverMsg.Error.GetMessage(), "", c.URL, serverMsg.Error.GetConnection())
	}

	if serverMsg.GetMessageType() != messages.Server_ReadData {
		c.setErrored()
		return nil, 0, common.NewConnectionError(fmt.Sprintf("Expected MessageType: %v. Received %v", messages.Server_ReadData, serverMsg.GetMessageType()), "", c.URL, false)
	}

	data := serverMsg.ReadData.GetData()
	index := serverMsg.ReadData.GetIndex()
	return data, index, nil
}

func (c *Connection) SeekIndex(fileId uint64, index uint64) *common.ConnectionError {
	respCh := c.writeSeekIndex(c.nextMsgId(), fileId, index)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		return common.NewConnectionError(serverMsg.Error.GetMessage(), "", c.URL, serverMsg.Error.GetConnection())
	}

	if serverMsg.GetMessageType() != messages.Server_FileIndex {
		return common.NewConnectionError(fmt.Sprintf("Expected MessageType: %v. Received %v", messages.Server_FileIndex, serverMsg.GetMessageType()), "", c.URL, false)
	}

	if index != uint64(serverMsg.FileIndex.GetIndex()) {
		return common.NewConnectionError(fmt.Sprintf("Expected index: %d. Received %d", index, serverMsg.FileIndex.GetIndex()), "", c.URL, false)
	}

	return nil
}

func (c *Connection) ValidateLeader(name string, index uint64) bool {
	respCh := c.writeViableLeader(c.nextMsgId(), name, index)
	serverMsg := <-respCh

	if serverMsg.GetMessageType() == messages.Server_Error {
		c.log.Panicf("Failed to submit (to %s)) ValidateLeader: %s", c.URL, serverMsg.Error.GetMessage())
	}

	if serverMsg.GetMessageType() != messages.Server_ImpeachLeader {
		c.log.Panicf("Expected MessageType: %v. Received %v", messages.Server_ImpeachLeader, serverMsg.GetMessageType())
	}

	return serverMsg.ImpeachLeader.GetImpeach()
}

func (c *Connection) Close() {
	c.closeOnce.Do(func() {
		c.conn.Close()
		close(c.doneCh)
		c.watchDog.Reset(1)
	})
}

func (c *Connection) Errored() bool {
	if c == nil {
		return false
	}

	return atomic.LoadUint32(&c.errored) == errored
}

func (c *Connection) setErrored() {
	atomic.StoreUint32(&c.errored, errored)
	c.Close()
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

	server := new(messages.Server)
	err = server.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (c *Connection) writeCore() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.doneCh:
			return
		case <-ticker.C:
			c.writePing()
		case msg, ok := <-c.writeCh:
			if !ok {
				return
			}
			c.lock.Lock()
			c.clientMap[msg.msg.GetMessageId()] = msg.respCh
			c.lock.Unlock()

			c.writeMessage(msg.msg)
		}
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

		c.watchDog.Reset(watchDogReset)
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
		return
	}

	if err = c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		c.log.Errorf("Failed to write to %s: %v", c.URL, err)
		c.submitWebsocketError(err, msg.GetMessageId())
		return
	}
}

func (c *Connection) writeFetchFile(msgId, fileId uint64, name string, create bool) <-chan *messages.Server {
	messageType := messages.Client_FetchFile
	msg := &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		FetchFile: &messages.FetchFile{
			Name:   proto.String(name),
			FileId: proto.Uint64(fileId),
			Create: proto.Bool(create),
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

func (c *Connection) writeSeekIndex(msgId, fileId, index uint64) <-chan *messages.Server {
	messageType := messages.Client_SeekIndex
	msg := &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		SeekIndex: &messages.SeekIndex{
			FileId: proto.Uint64(fileId),
			Index:  proto.Uint64(index),
		},
	}

	respCh := make(chan *messages.Server, 1)
	c.writeCh <- clientMsgInfo{
		respCh: respCh,
		msg:    msg,
	}

	return respCh
}

func (c *Connection) writeViableLeader(msgId uint64, fileName string, index uint64) <-chan *messages.Server {
	messageType := messages.Client_ViableLeader
	msg := &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		ViableLeader: &messages.ViableLeader{
			Name:  proto.String(fileName),
			Index: proto.Uint64(index),
		},
	}

	respCh := make(chan *messages.Server, 1)
	c.writeCh <- clientMsgInfo{
		respCh: respCh,
		msg:    msg,
	}

	return respCh
}

func (c *Connection) writeImpeach(msgId uint64, name string) {
	messageType := messages.Client_Impeach
	msg := &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		Impeach: &messages.Impeach{
			Name: proto.String(name),
		},
	}

	respCh := make(chan *messages.Server, 1)
	c.writeCh <- clientMsgInfo{
		respCh: respCh,
		msg:    msg,
	}
}

func (c *Connection) writePing() {
	err := c.conn.WriteControl(websocket.PingMessage, []byte("some-message"), time.Now().Add(time.Second))
	if err != nil {
		c.Close()
	}
}

func (c *Connection) watchDogCore() {
	if _, ok := <-c.watchDog.C; !ok {
		return
	}

	c.log.Errorf("Watchdog timer expired")
	c.Close()
}
