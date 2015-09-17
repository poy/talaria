package broker

import (
	"fmt"

	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/messages"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
)

type Connection struct {
	log       logging.Logger
	conn      *websocket.Conn
	messageId uint64
}

func NewConnection(URL string) (*Connection, error) {
	log := logging.Log("Connection")
	conn, _, err := websocket.DefaultDialer.Dial(URL, nil)
	if err != nil {
		return nil, err
	}

	return &Connection{
		log:  log,
		conn: conn,
	}, nil
}

func (c *Connection) FetchFile(fileId uint64, name string) *FetchFileError {
	err := c.writeFetchFile(c.nextMsgId(), fileId, name)
	if err != nil {
		return NewFetchFileError(err.Error(), "")
	}

	serverMsg, err := c.readMessage()
	if err != nil {
		return NewFetchFileError(err.Error(), "")
	}

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
	err := c.writeWriteToFile(c.nextMsgId(), fileId, data)
	if err != nil {
		return 0, err
	}

	serverMsg, err := c.readMessage()
	if err != nil {
		return 0, err
	}

	if serverMsg.GetMessageType() == messages.Server_Error {
		return 0, fmt.Errorf(serverMsg.Error.GetMessage())
	}

	if serverMsg.GetMessageType() != messages.Server_FileOffset {
		return 0, fmt.Errorf("Unexpected MessageType: %v", serverMsg.GetMessageType())
	}

	return serverMsg.FileOffset.GetOffset(), nil
}

func (c *Connection) ReadFromFile(fileId uint64) ([]byte, error) {
	err := c.writeReadFromFile(c.nextMsgId(), fileId)
	if err != nil {
		return nil, err
	}

	serverMsg, err := c.readMessage()
	if err != nil {
		return nil, err
	}

	if serverMsg.GetMessageType() == messages.Server_Error {
		return nil, fmt.Errorf(serverMsg.Error.GetMessage())
	}

	if serverMsg.GetMessageType() != messages.Server_ReadData {
		return nil, fmt.Errorf("Unexpected MessageType: %v", serverMsg.GetMessageType())
	}

	return serverMsg.ReadData.GetData(), nil
}

func (c *Connection) Close() {
	c.conn.Close()
}

func (c *Connection) nextMsgId() uint64 {
	c.messageId++
	return c.messageId
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

func (c *Connection) writeMessage(msg *messages.Client) error {
	data, err := msg.Marshal()
	if err != nil {
		c.log.Panic("Unable to marshal message", err)
	}

	return c.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (c *Connection) writeFetchFile(msgId, fileId uint64, name string) error {
	messageType := messages.Client_FetchFile
	msg := &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		FetchFile: &messages.FetchFile{
			Name:   proto.String(name),
			FileId: proto.Uint64(fileId),
		},
	}
	return c.writeMessage(msg)
}

func (c *Connection) writeWriteToFile(msgId, fileId uint64, data []byte) error {
	messageType := messages.Client_WriteToFile
	msg := &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		WriteToFile: &messages.WriteToFile{
			FileId: proto.Uint64(fileId),
			Data:   data,
		},
	}
	return c.writeMessage(msg)
}

func (c *Connection) writeReadFromFile(msgId, fileId uint64) error {
	messageType := messages.Client_ReadFromFile
	msg := &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		ReadFromFile: &messages.ReadFromFile{
			FileId: proto.Uint64(fileId),
		},
	}
	return c.writeMessage(msg)
}
