package broker

type FetchFileError struct {
	errMessage string
	Uri        string
}

func NewFetchFileError(msg, uri string) *FetchFileError {
	return &FetchFileError{
		errMessage: msg,
		Uri:        uri,
	}
}

func (f *FetchFileError) Error() string {
	if f == nil {
		return ""
	}
	return f.errMessage
}
