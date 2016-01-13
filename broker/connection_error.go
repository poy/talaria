package broker

type ConnectionError struct {
	errMessage     string
	Uri            string
	ConnURL        string
	WebsocketError bool
}

func NewConnectionError(msg, uri, connURL string, websocketErr bool) *ConnectionError {
	return &ConnectionError{
		errMessage:     msg,
		Uri:            uri,
		ConnURL:        connURL,
		WebsocketError: websocketErr,
	}
}

func (c *ConnectionError) Error() string {
	if c == nil {
		return ""
	}
	return c.errMessage
}
