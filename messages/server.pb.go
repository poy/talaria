// Code generated by protoc-gen-gogo.
// source: server.proto
// DO NOT EDIT!

package messages

import proto "github.com/gogo/protobuf/proto"
import math "math"

// discarding unused import gogoproto "github.com/gogo/protobuf/gogoproto/gogo.pb"

import io "io"
import fmt "fmt"
import github_com_gogo_protobuf_proto "github.com/gogo/protobuf/proto"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

type Server_MessageType int32

const (
	Server_Error        Server_MessageType = 1
	Server_FileLocation Server_MessageType = 2
	Server_FileOffset   Server_MessageType = 3
	Server_ReadData     Server_MessageType = 4
)

var Server_MessageType_name = map[int32]string{
	1: "Error",
	2: "FileLocation",
	3: "FileOffset",
	4: "ReadData",
}
var Server_MessageType_value = map[string]int32{
	"Error":        1,
	"FileLocation": 2,
	"FileOffset":   3,
	"ReadData":     4,
}

func (x Server_MessageType) Enum() *Server_MessageType {
	p := new(Server_MessageType)
	*p = x
	return p
}
func (x Server_MessageType) String() string {
	return proto.EnumName(Server_MessageType_name, int32(x))
}
func (x *Server_MessageType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(Server_MessageType_value, data, "Server_MessageType")
	if err != nil {
		return err
	}
	*x = Server_MessageType(value)
	return nil
}

type Server struct {
	MessageId        *uint64             `protobuf:"varint,1,req,name=messageId" json:"messageId,omitempty"`
	MessageType      *Server_MessageType `protobuf:"varint,2,req,name=messageType,enum=messages.Server_MessageType" json:"messageType,omitempty"`
	Error            *Error              `protobuf:"bytes,3,opt,name=error" json:"error,omitempty"`
	FileLocation     *FileLocation       `protobuf:"bytes,4,opt,name=fileLocation" json:"fileLocation,omitempty"`
	FileOffset       *FileOffset         `protobuf:"bytes,5,opt,name=fileOffset" json:"fileOffset,omitempty"`
	ReadData         *ReadData           `protobuf:"bytes,6,opt,name=readData" json:"readData,omitempty"`
	XXX_unrecognized []byte              `json:"-"`
}

func (m *Server) Reset()         { *m = Server{} }
func (m *Server) String() string { return proto.CompactTextString(m) }
func (*Server) ProtoMessage()    {}

func (m *Server) GetMessageId() uint64 {
	if m != nil && m.MessageId != nil {
		return *m.MessageId
	}
	return 0
}

func (m *Server) GetMessageType() Server_MessageType {
	if m != nil && m.MessageType != nil {
		return *m.MessageType
	}
	return Server_Error
}

func (m *Server) GetError() *Error {
	if m != nil {
		return m.Error
	}
	return nil
}

func (m *Server) GetFileLocation() *FileLocation {
	if m != nil {
		return m.FileLocation
	}
	return nil
}

func (m *Server) GetFileOffset() *FileOffset {
	if m != nil {
		return m.FileOffset
	}
	return nil
}

func (m *Server) GetReadData() *ReadData {
	if m != nil {
		return m.ReadData
	}
	return nil
}

type Error struct {
	Message          *string `protobuf:"bytes,1,req,name=message" json:"message,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Error) Reset()         { *m = Error{} }
func (m *Error) String() string { return proto.CompactTextString(m) }
func (*Error) ProtoMessage()    {}

func (m *Error) GetMessage() string {
	if m != nil && m.Message != nil {
		return *m.Message
	}
	return ""
}

type FileLocation struct {
	Local            *bool   `protobuf:"varint,1,req,name=local" json:"local,omitempty"`
	Uri              *string `protobuf:"bytes,2,opt,name=uri" json:"uri,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *FileLocation) Reset()         { *m = FileLocation{} }
func (m *FileLocation) String() string { return proto.CompactTextString(m) }
func (*FileLocation) ProtoMessage()    {}

func (m *FileLocation) GetLocal() bool {
	if m != nil && m.Local != nil {
		return *m.Local
	}
	return false
}

func (m *FileLocation) GetUri() string {
	if m != nil && m.Uri != nil {
		return *m.Uri
	}
	return ""
}

type FileOffset struct {
	Offset           *int64 `protobuf:"varint,1,req,name=offset" json:"offset,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *FileOffset) Reset()         { *m = FileOffset{} }
func (m *FileOffset) String() string { return proto.CompactTextString(m) }
func (*FileOffset) ProtoMessage()    {}

func (m *FileOffset) GetOffset() int64 {
	if m != nil && m.Offset != nil {
		return *m.Offset
	}
	return 0
}

type ReadData struct {
	Data             []byte `protobuf:"bytes,1,req,name=data" json:"data,omitempty"`
	Offset           *int64 `protobuf:"varint,2,req,name=offset" json:"offset,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *ReadData) Reset()         { *m = ReadData{} }
func (m *ReadData) String() string { return proto.CompactTextString(m) }
func (*ReadData) ProtoMessage()    {}

func (m *ReadData) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *ReadData) GetOffset() int64 {
	if m != nil && m.Offset != nil {
		return *m.Offset
	}
	return 0
}

func init() {
	proto.RegisterEnum("messages.Server_MessageType", Server_MessageType_name, Server_MessageType_value)
}
func (m *Server) Unmarshal(data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MessageId", wireType)
			}
			var v uint64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.MessageId = &v
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MessageType", wireType)
			}
			var v Server_MessageType
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (Server_MessageType(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.MessageType = &v
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Error", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Error == nil {
				m.Error = &Error{}
			}
			if err := m.Error.Unmarshal(data[index:postIndex]); err != nil {
				return err
			}
			index = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field FileLocation", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.FileLocation == nil {
				m.FileLocation = &FileLocation{}
			}
			if err := m.FileLocation.Unmarshal(data[index:postIndex]); err != nil {
				return err
			}
			index = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field FileOffset", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.FileOffset == nil {
				m.FileOffset = &FileOffset{}
			}
			if err := m.FileOffset.Unmarshal(data[index:postIndex]); err != nil {
				return err
			}
			index = postIndex
		case 6:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ReadData", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.ReadData == nil {
				m.ReadData = &ReadData{}
			}
			if err := m.ReadData.Unmarshal(data[index:postIndex]); err != nil {
				return err
			}
			index = postIndex
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			index -= sizeOfWire
			skippy, err := github_com_gogo_protobuf_proto.Skip(data[index:])
			if err != nil {
				return err
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, data[index:index+skippy]...)
			index += skippy
		}
	}
	return nil
}
func (m *Error) Unmarshal(data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Message", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + int(stringLen)
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			s := string(data[index:postIndex])
			m.Message = &s
			index = postIndex
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			index -= sizeOfWire
			skippy, err := github_com_gogo_protobuf_proto.Skip(data[index:])
			if err != nil {
				return err
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, data[index:index+skippy]...)
			index += skippy
		}
	}
	return nil
}
func (m *FileLocation) Unmarshal(data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Local", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			b := bool(v != 0)
			m.Local = &b
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Uri", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + int(stringLen)
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			s := string(data[index:postIndex])
			m.Uri = &s
			index = postIndex
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			index -= sizeOfWire
			skippy, err := github_com_gogo_protobuf_proto.Skip(data[index:])
			if err != nil {
				return err
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, data[index:index+skippy]...)
			index += skippy
		}
	}
	return nil
}
func (m *FileOffset) Unmarshal(data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Offset", wireType)
			}
			var v int64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Offset = &v
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			index -= sizeOfWire
			skippy, err := github_com_gogo_protobuf_proto.Skip(data[index:])
			if err != nil {
				return err
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, data[index:index+skippy]...)
			index += skippy
		}
	}
	return nil
}
func (m *ReadData) Unmarshal(data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Data = append([]byte{}, data[index:postIndex]...)
			index = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Offset", wireType)
			}
			var v int64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Offset = &v
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			index -= sizeOfWire
			skippy, err := github_com_gogo_protobuf_proto.Skip(data[index:])
			if err != nil {
				return err
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, data[index:index+skippy]...)
			index += skippy
		}
	}
	return nil
}
func (m *Server) Size() (n int) {
	var l int
	_ = l
	if m.MessageId != nil {
		n += 1 + sovServer(uint64(*m.MessageId))
	}
	if m.MessageType != nil {
		n += 1 + sovServer(uint64(*m.MessageType))
	}
	if m.Error != nil {
		l = m.Error.Size()
		n += 1 + l + sovServer(uint64(l))
	}
	if m.FileLocation != nil {
		l = m.FileLocation.Size()
		n += 1 + l + sovServer(uint64(l))
	}
	if m.FileOffset != nil {
		l = m.FileOffset.Size()
		n += 1 + l + sovServer(uint64(l))
	}
	if m.ReadData != nil {
		l = m.ReadData.Size()
		n += 1 + l + sovServer(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *Error) Size() (n int) {
	var l int
	_ = l
	if m.Message != nil {
		l = len(*m.Message)
		n += 1 + l + sovServer(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *FileLocation) Size() (n int) {
	var l int
	_ = l
	if m.Local != nil {
		n += 2
	}
	if m.Uri != nil {
		l = len(*m.Uri)
		n += 1 + l + sovServer(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *FileOffset) Size() (n int) {
	var l int
	_ = l
	if m.Offset != nil {
		n += 1 + sovServer(uint64(*m.Offset))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *ReadData) Size() (n int) {
	var l int
	_ = l
	if m.Data != nil {
		l = len(m.Data)
		n += 1 + l + sovServer(uint64(l))
	}
	if m.Offset != nil {
		n += 1 + sovServer(uint64(*m.Offset))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovServer(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozServer(x uint64) (n int) {
	return sovServer(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Server) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *Server) MarshalTo(data []byte) (n int, err error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.MessageId != nil {
		data[i] = 0x8
		i++
		i = encodeVarintServer(data, i, uint64(*m.MessageId))
	}
	if m.MessageType != nil {
		data[i] = 0x10
		i++
		i = encodeVarintServer(data, i, uint64(*m.MessageType))
	}
	if m.Error != nil {
		data[i] = 0x1a
		i++
		i = encodeVarintServer(data, i, uint64(m.Error.Size()))
		n1, err := m.Error.MarshalTo(data[i:])
		if err != nil {
			return 0, err
		}
		i += n1
	}
	if m.FileLocation != nil {
		data[i] = 0x22
		i++
		i = encodeVarintServer(data, i, uint64(m.FileLocation.Size()))
		n2, err := m.FileLocation.MarshalTo(data[i:])
		if err != nil {
			return 0, err
		}
		i += n2
	}
	if m.FileOffset != nil {
		data[i] = 0x2a
		i++
		i = encodeVarintServer(data, i, uint64(m.FileOffset.Size()))
		n3, err := m.FileOffset.MarshalTo(data[i:])
		if err != nil {
			return 0, err
		}
		i += n3
	}
	if m.ReadData != nil {
		data[i] = 0x32
		i++
		i = encodeVarintServer(data, i, uint64(m.ReadData.Size()))
		n4, err := m.ReadData.MarshalTo(data[i:])
		if err != nil {
			return 0, err
		}
		i += n4
	}
	if m.XXX_unrecognized != nil {
		i += copy(data[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *Error) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *Error) MarshalTo(data []byte) (n int, err error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Message != nil {
		data[i] = 0xa
		i++
		i = encodeVarintServer(data, i, uint64(len(*m.Message)))
		i += copy(data[i:], *m.Message)
	}
	if m.XXX_unrecognized != nil {
		i += copy(data[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *FileLocation) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *FileLocation) MarshalTo(data []byte) (n int, err error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Local != nil {
		data[i] = 0x8
		i++
		if *m.Local {
			data[i] = 1
		} else {
			data[i] = 0
		}
		i++
	}
	if m.Uri != nil {
		data[i] = 0x12
		i++
		i = encodeVarintServer(data, i, uint64(len(*m.Uri)))
		i += copy(data[i:], *m.Uri)
	}
	if m.XXX_unrecognized != nil {
		i += copy(data[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *FileOffset) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *FileOffset) MarshalTo(data []byte) (n int, err error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Offset != nil {
		data[i] = 0x8
		i++
		i = encodeVarintServer(data, i, uint64(*m.Offset))
	}
	if m.XXX_unrecognized != nil {
		i += copy(data[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *ReadData) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *ReadData) MarshalTo(data []byte) (n int, err error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Data != nil {
		data[i] = 0xa
		i++
		i = encodeVarintServer(data, i, uint64(len(m.Data)))
		i += copy(data[i:], m.Data)
	}
	if m.Offset != nil {
		data[i] = 0x10
		i++
		i = encodeVarintServer(data, i, uint64(*m.Offset))
	}
	if m.XXX_unrecognized != nil {
		i += copy(data[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func encodeFixed64Server(data []byte, offset int, v uint64) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	data[offset+4] = uint8(v >> 32)
	data[offset+5] = uint8(v >> 40)
	data[offset+6] = uint8(v >> 48)
	data[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32Server(data []byte, offset int, v uint32) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintServer(data []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		data[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	data[offset] = uint8(v)
	return offset + 1
}
