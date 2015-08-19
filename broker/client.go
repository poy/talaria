package broker

import (
	"fmt"

	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/messages"
	"github.com/gorilla/websocket"
)

type Client struct {
	log        logging.Logger
	conn       *websocket.Conn
	nextFileId uint64
}

func NewClient(URL string) *Client {
	log := logging.Log("Client")
	conn, _, err := websocket.DefaultDialer.Dial(URL, nil)
	if err != nil {
		log.Panic("Failed to connect", err)
	}

	return &Client{
		log:  log,
		conn: conn,
	}
}

func (c *Client) FetchFile(name string) (uint64, error) {
	err := c.writeFetchFile(c.nextId(), name)
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

	if serverMsg.GetMessageType() != messages.Server_FileLocation {
		return 0, fmt.Errorf("Unexpected MessageType: %v", serverMsg.GetMessageType())
	}

	return serverMsg.FileLocation.GetFileId(), nil
}

func (c *Client) WriteToFile(fileId uint64, data []byte) (int64, error) {
	err := c.writeWriteToFile(c.nextId(), fileId, data)
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

func (c *Client) ReadFromFile(fileId uint64) ([]byte, error) {
	err := c.writeReadFromFile(c.nextId(), fileId)
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

func (c *Client) nextId() uint64 {
	c.nextFileId++
	return c.nextFileId
}

func (c *Client) readMessage() (*messages.Server, error) {
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

func (c *Client) writeMessage(msg *messages.Client) error {
	data, err := msg.Marshal()
	if err != nil {
		c.log.Panic("Unable to marshal message", err)
	}

	return c.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (c *Client) writeFetchFile(id uint64, name string) error {
	messageType := messages.Client_FetchFile
	msg := &messages.Client{
		MessageType: &messageType,
		MessageId:   &id,
		FetchFile: &messages.FetchFile{
			Name: &name,
		},
	}
	return c.writeMessage(msg)
}

func (c *Client) writeWriteToFile(msgId, fileId uint64, data []byte) error {
	messageType := messages.Client_WriteToFile
	msg := &messages.Client{
		MessageType: &messageType,
		MessageId:   &msgId,
		WriteToFile: &messages.WriteToFile{
			FileId: &fileId,
			Data:   data,
		},
	}
	return c.writeMessage(msg)
}

func (c *Client) writeReadFromFile(msgId, fileId uint64) error {
	messageType := messages.Client_ReadFromFile
	msg := &messages.Client{
		MessageType: &messageType,
		MessageId:   &msgId,
		ReadFromFile: &messages.ReadFromFile{
			FileId: &fileId,
		},
	}
	return c.writeMessage(msg)
}
