package broker

type ConnectionError struct {
	errMessage     string
	Uri            string
	WebsocketError bool
}

func NewConnectionError(msg, uri string, websocketErr bool) *ConnectionError {
	return &ConnectionError{
		errMessage:     msg,
		Uri:            uri,
		WebsocketError: websocketErr,
	}
}

func (c *ConnectionError) Error() string {
	if c == nil {
		return ""
	}
	return c.errMessage
}
