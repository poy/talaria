// Code generated by protoc-gen-go.
// source: node.proto
// DO NOT EDIT!

/*
Package intra is a generated protocol buffer package.

It is generated from these files:
	node.proto

It has these top-level messages:
	CreateInfo
	CreateResponse
	PeerInfo
	LeaderRequest
	LeaderResponse
	AppendEntriesRequest
	AppendEntriesResponse
	RaftLog
	RequestVoteRequest
	RequestVoteResponse
	UpdateResponse
	UpdateConfigRequest
	UpdateConfigResponse
	StatusRequest
	StatusResponse
	StatusBufferInfo
*/
package intra

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type RaftLog_LogType int32

const (
	RaftLog_LogCommand    RaftLog_LogType = 0
	RaftLog_LogNoop       RaftLog_LogType = 1
	RaftLog_LogAddPeer    RaftLog_LogType = 2
	RaftLog_LogRemovePeer RaftLog_LogType = 3
	RaftLog_LogBarrier    RaftLog_LogType = 4
)

var RaftLog_LogType_name = map[int32]string{
	0: "LogCommand",
	1: "LogNoop",
	2: "LogAddPeer",
	3: "LogRemovePeer",
	4: "LogBarrier",
}
var RaftLog_LogType_value = map[string]int32{
	"LogCommand":    0,
	"LogNoop":       1,
	"LogAddPeer":    2,
	"LogRemovePeer": 3,
	"LogBarrier":    4,
}

func (x RaftLog_LogType) String() string {
	return proto.EnumName(RaftLog_LogType_name, int32(x))
}
func (RaftLog_LogType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{7, 0} }

type CreateInfo struct {
	Name  string      `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Peers []*PeerInfo `protobuf:"bytes,2,rep,name=peers" json:"peers,omitempty"`
}

func (m *CreateInfo) Reset()                    { *m = CreateInfo{} }
func (m *CreateInfo) String() string            { return proto.CompactTextString(m) }
func (*CreateInfo) ProtoMessage()               {}
func (*CreateInfo) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *CreateInfo) GetPeers() []*PeerInfo {
	if m != nil {
		return m.Peers
	}
	return nil
}

type CreateResponse struct {
}

func (m *CreateResponse) Reset()                    { *m = CreateResponse{} }
func (m *CreateResponse) String() string            { return proto.CompactTextString(m) }
func (*CreateResponse) ProtoMessage()               {}
func (*CreateResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

type PeerInfo struct {
	Addr string `protobuf:"bytes,1,opt,name=addr" json:"addr,omitempty"`
}

func (m *PeerInfo) Reset()                    { *m = PeerInfo{} }
func (m *PeerInfo) String() string            { return proto.CompactTextString(m) }
func (*PeerInfo) ProtoMessage()               {}
func (*PeerInfo) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

type LeaderRequest struct {
	Name string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
}

func (m *LeaderRequest) Reset()                    { *m = LeaderRequest{} }
func (m *LeaderRequest) String() string            { return proto.CompactTextString(m) }
func (*LeaderRequest) ProtoMessage()               {}
func (*LeaderRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

type LeaderResponse struct {
	Addr string `protobuf:"bytes,1,opt,name=addr" json:"addr,omitempty"`
}

func (m *LeaderResponse) Reset()                    { *m = LeaderResponse{} }
func (m *LeaderResponse) String() string            { return proto.CompactTextString(m) }
func (*LeaderResponse) ProtoMessage()               {}
func (*LeaderResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

type AppendEntriesRequest struct {
	BufferName        string     `protobuf:"bytes,1,opt,name=BufferName,json=bufferName" json:"BufferName,omitempty"`
	Term              uint64     `protobuf:"varint,2,opt,name=Term,json=term" json:"Term,omitempty"`
	Leader            []byte     `protobuf:"bytes,3,opt,name=Leader,json=leader,proto3" json:"Leader,omitempty"`
	PrevLogEntry      uint64     `protobuf:"varint,4,opt,name=PrevLogEntry,json=prevLogEntry" json:"PrevLogEntry,omitempty"`
	PrevLogTerm       uint64     `protobuf:"varint,5,opt,name=PrevLogTerm,json=prevLogTerm" json:"PrevLogTerm,omitempty"`
	Entries           []*RaftLog `protobuf:"bytes,6,rep,name=Entries,json=entries" json:"Entries,omitempty"`
	LeaderCommitIndex uint64     `protobuf:"varint,7,opt,name=LeaderCommitIndex,json=leaderCommitIndex" json:"LeaderCommitIndex,omitempty"`
}

func (m *AppendEntriesRequest) Reset()                    { *m = AppendEntriesRequest{} }
func (m *AppendEntriesRequest) String() string            { return proto.CompactTextString(m) }
func (*AppendEntriesRequest) ProtoMessage()               {}
func (*AppendEntriesRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

func (m *AppendEntriesRequest) GetEntries() []*RaftLog {
	if m != nil {
		return m.Entries
	}
	return nil
}

type AppendEntriesResponse struct {
	Term           uint64 `protobuf:"varint,1,opt,name=Term,json=term" json:"Term,omitempty"`
	LastLog        uint64 `protobuf:"varint,2,opt,name=LastLog,json=lastLog" json:"LastLog,omitempty"`
	Success        bool   `protobuf:"varint,3,opt,name=Success,json=success" json:"Success,omitempty"`
	NoRetryBackoff bool   `protobuf:"varint,4,opt,name=NoRetryBackoff,json=noRetryBackoff" json:"NoRetryBackoff,omitempty"`
}

func (m *AppendEntriesResponse) Reset()                    { *m = AppendEntriesResponse{} }
func (m *AppendEntriesResponse) String() string            { return proto.CompactTextString(m) }
func (*AppendEntriesResponse) ProtoMessage()               {}
func (*AppendEntriesResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{6} }

type RaftLog struct {
	Index uint64          `protobuf:"varint,1,opt,name=Index,json=index" json:"Index,omitempty"`
	Term  uint64          `protobuf:"varint,2,opt,name=Term,json=term" json:"Term,omitempty"`
	Type  RaftLog_LogType `protobuf:"varint,3,opt,name=Type,json=type,enum=intra.RaftLog_LogType" json:"Type,omitempty"`
	Data  []byte          `protobuf:"bytes,4,opt,name=Data,json=data,proto3" json:"Data,omitempty"`
}

func (m *RaftLog) Reset()                    { *m = RaftLog{} }
func (m *RaftLog) String() string            { return proto.CompactTextString(m) }
func (*RaftLog) ProtoMessage()               {}
func (*RaftLog) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{7} }

type RequestVoteRequest struct {
	BufferName   string `protobuf:"bytes,1,opt,name=BufferName,json=bufferName" json:"BufferName,omitempty"`
	Term         uint64 `protobuf:"varint,2,opt,name=Term,json=term" json:"Term,omitempty"`
	Candidate    []byte `protobuf:"bytes,3,opt,name=Candidate,json=candidate,proto3" json:"Candidate,omitempty"`
	LastLogIndex uint64 `protobuf:"varint,4,opt,name=LastLogIndex,json=lastLogIndex" json:"LastLogIndex,omitempty"`
	LastLogTerm  uint64 `protobuf:"varint,5,opt,name=LastLogTerm,json=lastLogTerm" json:"LastLogTerm,omitempty"`
}

func (m *RequestVoteRequest) Reset()                    { *m = RequestVoteRequest{} }
func (m *RequestVoteRequest) String() string            { return proto.CompactTextString(m) }
func (*RequestVoteRequest) ProtoMessage()               {}
func (*RequestVoteRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{8} }

type RequestVoteResponse struct {
	Term    uint64 `protobuf:"varint,1,opt,name=Term,json=term" json:"Term,omitempty"`
	Peers   []byte `protobuf:"bytes,2,opt,name=Peers,json=peers,proto3" json:"Peers,omitempty"`
	Granted bool   `protobuf:"varint,3,opt,name=Granted,json=granted" json:"Granted,omitempty"`
}

func (m *RequestVoteResponse) Reset()                    { *m = RequestVoteResponse{} }
func (m *RequestVoteResponse) String() string            { return proto.CompactTextString(m) }
func (*RequestVoteResponse) ProtoMessage()               {}
func (*RequestVoteResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{9} }

type UpdateResponse struct {
	Dropped bool `protobuf:"varint,1,opt,name=dropped" json:"dropped,omitempty"`
}

func (m *UpdateResponse) Reset()                    { *m = UpdateResponse{} }
func (m *UpdateResponse) String() string            { return proto.CompactTextString(m) }
func (*UpdateResponse) ProtoMessage()               {}
func (*UpdateResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{10} }

type UpdateConfigRequest struct {
	Name          string   `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	ExpectedNodes []string `protobuf:"bytes,2,rep,name=expectedNodes" json:"expectedNodes,omitempty"`
}

func (m *UpdateConfigRequest) Reset()                    { *m = UpdateConfigRequest{} }
func (m *UpdateConfigRequest) String() string            { return proto.CompactTextString(m) }
func (*UpdateConfigRequest) ProtoMessage()               {}
func (*UpdateConfigRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{11} }

type UpdateConfigResponse struct {
}

func (m *UpdateConfigResponse) Reset()                    { *m = UpdateConfigResponse{} }
func (m *UpdateConfigResponse) String() string            { return proto.CompactTextString(m) }
func (*UpdateConfigResponse) ProtoMessage()               {}
func (*UpdateConfigResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{12} }

type StatusRequest struct {
}

func (m *StatusRequest) Reset()                    { *m = StatusRequest{} }
func (m *StatusRequest) String() string            { return proto.CompactTextString(m) }
func (*StatusRequest) ProtoMessage()               {}
func (*StatusRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{13} }

type StatusResponse struct {
	ExternalAddr string              `protobuf:"bytes,1,opt,name=externalAddr" json:"externalAddr,omitempty"`
	Buffers      []*StatusBufferInfo `protobuf:"bytes,2,rep,name=buffers" json:"buffers,omitempty"`
}

func (m *StatusResponse) Reset()                    { *m = StatusResponse{} }
func (m *StatusResponse) String() string            { return proto.CompactTextString(m) }
func (*StatusResponse) ProtoMessage()               {}
func (*StatusResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{14} }

func (m *StatusResponse) GetBuffers() []*StatusBufferInfo {
	if m != nil {
		return m.Buffers
	}
	return nil
}

type StatusBufferInfo struct {
	Name          string   `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	ExpectedNodes []string `protobuf:"bytes,2,rep,name=expectedNodes" json:"expectedNodes,omitempty"`
}

func (m *StatusBufferInfo) Reset()                    { *m = StatusBufferInfo{} }
func (m *StatusBufferInfo) String() string            { return proto.CompactTextString(m) }
func (*StatusBufferInfo) ProtoMessage()               {}
func (*StatusBufferInfo) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{15} }

func init() {
	proto.RegisterType((*CreateInfo)(nil), "intra.CreateInfo")
	proto.RegisterType((*CreateResponse)(nil), "intra.CreateResponse")
	proto.RegisterType((*PeerInfo)(nil), "intra.PeerInfo")
	proto.RegisterType((*LeaderRequest)(nil), "intra.LeaderRequest")
	proto.RegisterType((*LeaderResponse)(nil), "intra.LeaderResponse")
	proto.RegisterType((*AppendEntriesRequest)(nil), "intra.AppendEntriesRequest")
	proto.RegisterType((*AppendEntriesResponse)(nil), "intra.AppendEntriesResponse")
	proto.RegisterType((*RaftLog)(nil), "intra.RaftLog")
	proto.RegisterType((*RequestVoteRequest)(nil), "intra.RequestVoteRequest")
	proto.RegisterType((*RequestVoteResponse)(nil), "intra.RequestVoteResponse")
	proto.RegisterType((*UpdateResponse)(nil), "intra.UpdateResponse")
	proto.RegisterType((*UpdateConfigRequest)(nil), "intra.UpdateConfigRequest")
	proto.RegisterType((*UpdateConfigResponse)(nil), "intra.UpdateConfigResponse")
	proto.RegisterType((*StatusRequest)(nil), "intra.StatusRequest")
	proto.RegisterType((*StatusResponse)(nil), "intra.StatusResponse")
	proto.RegisterType((*StatusBufferInfo)(nil), "intra.StatusBufferInfo")
	proto.RegisterEnum("intra.RaftLog_LogType", RaftLog_LogType_name, RaftLog_LogType_value)
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion3

// Client API for Node service

type NodeClient interface {
	Create(ctx context.Context, in *CreateInfo, opts ...grpc.CallOption) (*CreateResponse, error)
	UpdateConfig(ctx context.Context, in *UpdateConfigRequest, opts ...grpc.CallOption) (*UpdateConfigResponse, error)
	Status(ctx context.Context, in *StatusRequest, opts ...grpc.CallOption) (*StatusResponse, error)
	Leader(ctx context.Context, in *LeaderRequest, opts ...grpc.CallOption) (*LeaderResponse, error)
}

type nodeClient struct {
	cc *grpc.ClientConn
}

func NewNodeClient(cc *grpc.ClientConn) NodeClient {
	return &nodeClient{cc}
}

func (c *nodeClient) Create(ctx context.Context, in *CreateInfo, opts ...grpc.CallOption) (*CreateResponse, error) {
	out := new(CreateResponse)
	err := grpc.Invoke(ctx, "/intra.Node/Create", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeClient) UpdateConfig(ctx context.Context, in *UpdateConfigRequest, opts ...grpc.CallOption) (*UpdateConfigResponse, error) {
	out := new(UpdateConfigResponse)
	err := grpc.Invoke(ctx, "/intra.Node/UpdateConfig", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeClient) Status(ctx context.Context, in *StatusRequest, opts ...grpc.CallOption) (*StatusResponse, error) {
	out := new(StatusResponse)
	err := grpc.Invoke(ctx, "/intra.Node/Status", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeClient) Leader(ctx context.Context, in *LeaderRequest, opts ...grpc.CallOption) (*LeaderResponse, error) {
	out := new(LeaderResponse)
	err := grpc.Invoke(ctx, "/intra.Node/Leader", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for Node service

type NodeServer interface {
	Create(context.Context, *CreateInfo) (*CreateResponse, error)
	UpdateConfig(context.Context, *UpdateConfigRequest) (*UpdateConfigResponse, error)
	Status(context.Context, *StatusRequest) (*StatusResponse, error)
	Leader(context.Context, *LeaderRequest) (*LeaderResponse, error)
}

func RegisterNodeServer(s *grpc.Server, srv NodeServer) {
	s.RegisterService(&_Node_serviceDesc, srv)
}

func _Node_Create_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateInfo)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).Create(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/intra.Node/Create",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).Create(ctx, req.(*CreateInfo))
	}
	return interceptor(ctx, in, info, handler)
}

func _Node_UpdateConfig_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateConfigRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).UpdateConfig(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/intra.Node/UpdateConfig",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).UpdateConfig(ctx, req.(*UpdateConfigRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Node_Status_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).Status(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/intra.Node/Status",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).Status(ctx, req.(*StatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Node_Leader_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).Leader(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/intra.Node/Leader",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).Leader(ctx, req.(*LeaderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Node_serviceDesc = grpc.ServiceDesc{
	ServiceName: "intra.Node",
	HandlerType: (*NodeServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Create",
			Handler:    _Node_Create_Handler,
		},
		{
			MethodName: "UpdateConfig",
			Handler:    _Node_UpdateConfig_Handler,
		},
		{
			MethodName: "Status",
			Handler:    _Node_Status_Handler,
		},
		{
			MethodName: "Leader",
			Handler:    _Node_Leader_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: fileDescriptor0,
}

// Client API for NodeRaft service

type NodeRaftClient interface {
	AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error)
	RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error)
}

type nodeRaftClient struct {
	cc *grpc.ClientConn
}

func NewNodeRaftClient(cc *grpc.ClientConn) NodeRaftClient {
	return &nodeRaftClient{cc}
}

func (c *nodeRaftClient) AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error) {
	out := new(AppendEntriesResponse)
	err := grpc.Invoke(ctx, "/intra.NodeRaft/AppendEntries", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeRaftClient) RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error) {
	out := new(RequestVoteResponse)
	err := grpc.Invoke(ctx, "/intra.NodeRaft/RequestVote", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for NodeRaft service

type NodeRaftServer interface {
	AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error)
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
}

func RegisterNodeRaftServer(s *grpc.Server, srv NodeRaftServer) {
	s.RegisterService(&_NodeRaft_serviceDesc, srv)
}

func _NodeRaft_AppendEntries_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendEntriesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeRaftServer).AppendEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/intra.NodeRaft/AppendEntries",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeRaftServer).AppendEntries(ctx, req.(*AppendEntriesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NodeRaft_RequestVote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RequestVoteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeRaftServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/intra.NodeRaft/RequestVote",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeRaftServer).RequestVote(ctx, req.(*RequestVoteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _NodeRaft_serviceDesc = grpc.ServiceDesc{
	ServiceName: "intra.NodeRaft",
	HandlerType: (*NodeRaftServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AppendEntries",
			Handler:    _NodeRaft_AppendEntries_Handler,
		},
		{
			MethodName: "RequestVote",
			Handler:    _NodeRaft_RequestVote_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: fileDescriptor0,
}

func init() { proto.RegisterFile("node.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 787 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x9c, 0x55, 0xcd, 0x8e, 0xe2, 0x46,
	0x10, 0x1e, 0x33, 0x06, 0x43, 0x01, 0x5e, 0xe8, 0x65, 0x26, 0x0e, 0x59, 0xad, 0x50, 0x67, 0x13,
	0xa1, 0x55, 0x84, 0x14, 0x12, 0x29, 0xe7, 0x19, 0x92, 0xac, 0x46, 0x42, 0x64, 0xe4, 0xdd, 0x44,
	0xda, 0x63, 0x2f, 0x5d, 0x20, 0x14, 0x70, 0x3b, 0xed, 0x66, 0x35, 0xdc, 0x72, 0xcb, 0x6b, 0xe4,
	0x01, 0xf2, 0x2c, 0x79, 0x9f, 0xdc, 0xa2, 0xfe, 0x31, 0xd8, 0xac, 0x35, 0x87, 0xb9, 0xb9, 0xab,
	0xaa, 0xbf, 0xaa, 0xfa, 0xba, 0xea, 0x33, 0x40, 0x22, 0x38, 0x4e, 0x52, 0x29, 0x94, 0x20, 0xf5,
	0x4d, 0xa2, 0x24, 0xa3, 0x6f, 0x00, 0x66, 0x12, 0x99, 0xc2, 0xbb, 0x64, 0x25, 0x08, 0x01, 0x3f,
	0x61, 0x3b, 0x8c, 0xbc, 0x91, 0x37, 0x6e, 0xc5, 0xe6, 0x9b, 0x7c, 0x05, 0xf5, 0x14, 0x51, 0x66,
	0x51, 0x6d, 0x74, 0x39, 0x6e, 0x4f, 0x9f, 0x4d, 0xcc, 0xc5, 0xc9, 0x3d, 0xa2, 0xd4, 0x77, 0x62,
	0xeb, 0xa5, 0x3d, 0x08, 0x2d, 0x50, 0x8c, 0x59, 0x2a, 0x92, 0x0c, 0xe9, 0x4b, 0x68, 0xe6, 0x41,
	0x1a, 0x98, 0x71, 0x2e, 0x73, 0x60, 0xfd, 0x4d, 0xbf, 0x84, 0xee, 0x1c, 0x19, 0x47, 0x19, 0xe3,
	0x1f, 0x7b, 0xcc, 0x54, 0x55, 0x76, 0xfa, 0x0a, 0xc2, 0x3c, 0xc8, 0xc2, 0x56, 0x42, 0xfd, 0x59,
	0x83, 0xc1, 0x4d, 0x9a, 0x62, 0xc2, 0x7f, 0x4a, 0x94, 0xdc, 0x60, 0x96, 0x43, 0xbe, 0x04, 0xb8,
	0xdd, 0xaf, 0x56, 0x28, 0x17, 0x27, 0x60, 0xf8, 0x70, 0xb4, 0x68, 0xb0, 0x77, 0x28, 0x77, 0x51,
	0x6d, 0xe4, 0x8d, 0xfd, 0xd8, 0x57, 0x28, 0x77, 0xe4, 0x1a, 0x1a, 0x36, 0x65, 0x74, 0x39, 0xf2,
	0xc6, 0x9d, 0xb8, 0xb1, 0x35, 0x27, 0x42, 0xa1, 0x73, 0x2f, 0xf1, 0xe3, 0x5c, 0xac, 0x75, 0x92,
	0x43, 0xe4, 0x9b, 0x3b, 0x9d, 0xb4, 0x60, 0x23, 0x23, 0x68, 0xbb, 0x18, 0x03, 0x5b, 0x37, 0x21,
	0xed, 0xf4, 0x64, 0x22, 0x63, 0x08, 0x5c, 0x8d, 0x51, 0xc3, 0x10, 0x1a, 0x3a, 0x42, 0x63, 0xb6,
	0x52, 0x73, 0xb1, 0x8e, 0x03, 0xb4, 0x6e, 0xf2, 0x0d, 0xf4, 0x6d, 0x1d, 0x33, 0xb1, 0xdb, 0x6d,
	0xd4, 0x5d, 0xc2, 0xf1, 0x21, 0x0a, 0x0c, 0x62, 0x7f, 0x7b, 0xee, 0xa0, 0x7f, 0x79, 0x70, 0x75,
	0x46, 0xc1, 0x89, 0x30, 0x53, 0x8c, 0x57, 0xe8, 0x31, 0x82, 0x60, 0xce, 0x32, 0x9d, 0xcf, 0xb5,
	0x1e, 0x6c, 0xed, 0x51, 0x7b, 0xde, 0xee, 0x97, 0x4b, 0xcc, 0x32, 0xd3, 0x7e, 0x33, 0x0e, 0x32,
	0x7b, 0x24, 0x5f, 0x43, 0xb8, 0x10, 0x31, 0x2a, 0x79, 0xb8, 0x65, 0xcb, 0xdf, 0xc5, 0x6a, 0x65,
	0x18, 0x68, 0xc6, 0x61, 0x52, 0xb2, 0xd2, 0x7f, 0x3d, 0x08, 0x5c, 0x33, 0x64, 0x00, 0x75, 0x5b,
	0xb7, 0x4d, 0x5e, 0xdf, 0xe8, 0x43, 0x25, 0xeb, 0xaf, 0xc1, 0x7f, 0x77, 0x48, 0xd1, 0x24, 0x0d,
	0xa7, 0xd7, 0x65, 0x52, 0x26, 0x9a, 0xbd, 0x43, 0x8a, 0xb1, 0xaf, 0x0e, 0xa9, 0xe9, 0xe8, 0x47,
	0xa6, 0x98, 0xc9, 0xdf, 0x89, 0x7d, 0xce, 0x14, 0xa3, 0xef, 0x21, 0x70, 0x41, 0x24, 0x04, 0x98,
	0x8b, 0xb5, 0x26, 0x87, 0x25, 0xbc, 0x77, 0x41, 0xda, 0xc6, 0xb5, 0x10, 0x22, 0xed, 0x79, 0xce,
	0x79, 0xc3, 0xb9, 0x9e, 0xcd, 0x5e, 0x8d, 0xf4, 0xa1, 0xab, 0x59, 0xc7, 0x9d, 0xf8, 0x88, 0xc6,
	0x74, 0xe9, 0x42, 0x6e, 0x99, 0x94, 0x1b, 0x94, 0x3d, 0x9f, 0xfe, 0xe3, 0x01, 0x71, 0x03, 0xf5,
	0x9b, 0xd0, 0x03, 0xfe, 0xf4, 0xd9, 0x7a, 0x01, 0xad, 0x19, 0x4b, 0xf8, 0x86, 0x33, 0x85, 0x6e,
	0xbc, 0x5a, 0xcb, 0xdc, 0xa0, 0x27, 0xcc, 0xbd, 0x8a, 0x25, 0xcd, 0x4d, 0xd8, 0xb6, 0x60, 0xd3,
	0x13, 0xe6, 0x62, 0x8a, 0x13, 0xb6, 0x3d, 0x99, 0xe8, 0x7b, 0x78, 0x5e, 0xaa, 0xf6, 0x91, 0x31,
	0x18, 0x40, 0xfd, 0xde, 0xed, 0xb6, 0x2e, 0xc5, 0xae, 0xb2, 0x1e, 0x81, 0x37, 0x92, 0x25, 0x0a,
	0x79, 0x3e, 0x02, 0x6b, 0x7b, 0xa4, 0xaf, 0x21, 0xfc, 0x35, 0xe5, 0x85, 0x25, 0xd7, 0xb1, 0x5c,
	0x8a, 0x34, 0x45, 0x6e, 0x80, 0x9b, 0x71, 0x7e, 0xa4, 0xbf, 0xc0, 0x73, 0x1b, 0x3b, 0x13, 0xc9,
	0x6a, 0xb3, 0x7e, 0x64, 0xc9, 0xc9, 0x2b, 0xe8, 0xe2, 0x43, 0x8a, 0x4b, 0x85, 0x7c, 0x21, 0x38,
	0x5a, 0xa9, 0x69, 0xc5, 0x65, 0x23, 0xbd, 0x86, 0x41, 0x19, 0xd0, 0xe9, 0xcc, 0x33, 0xe8, 0xbe,
	0x55, 0x4c, 0xed, 0xf3, 0xa5, 0xa7, 0x6b, 0x08, 0x73, 0x83, 0xab, 0x92, 0x42, 0x07, 0x1f, 0x14,
	0xca, 0x84, 0x6d, 0x6f, 0x4e, 0xda, 0x51, 0xb2, 0x91, 0x6f, 0x21, 0xb0, 0x8f, 0x97, 0x2b, 0xdd,
	0x67, 0x6e, 0x06, 0x2d, 0x96, 0x7d, 0x6a, 0xa3, 0x78, 0x79, 0x1c, 0x9d, 0x43, 0xef, 0xdc, 0xf9,
	0xf4, 0xfe, 0xa6, 0xff, 0x79, 0xe0, 0xeb, 0x2f, 0xf2, 0x3d, 0x34, 0xac, 0x94, 0x92, 0xbe, 0x2b,
	0xe1, 0x24, 0xd1, 0xc3, 0xab, 0x92, 0xe9, 0x48, 0xc2, 0x05, 0xb9, 0x83, 0x4e, 0x91, 0x1e, 0x32,
	0x74, 0x81, 0x15, 0x8f, 0x30, 0xfc, 0xa2, 0xd2, 0x77, 0x84, 0xfa, 0x01, 0x1a, 0xb6, 0x2f, 0x32,
	0x28, 0x71, 0x90, 0x5f, 0xbf, 0x3a, 0xb3, 0x16, 0x2f, 0x5a, 0xc9, 0x3a, 0x5e, 0x2c, 0x29, 0xfc,
	0xf1, 0x62, 0x59, 0xd2, 0xe9, 0xc5, 0xf4, 0x6f, 0x0f, 0x9a, 0xba, 0x77, 0xbd, 0xef, 0x64, 0x0e,
	0xdd, 0x92, 0x92, 0x91, 0xbc, 0xdc, 0x2a, 0x89, 0x1f, 0xbe, 0xa8, 0x76, 0x1e, 0x6b, 0xfa, 0x19,
	0xda, 0x85, 0x75, 0x20, 0x9f, 0xe7, 0xca, 0xf2, 0xc9, 0x42, 0x0f, 0x87, 0x55, 0xae, 0x1c, 0xe7,
	0x43, 0xc3, 0xfc, 0x37, 0xbf, 0xfb, 0x3f, 0x00, 0x00, 0xff, 0xff, 0x4f, 0x79, 0x1f, 0x9f, 0x45,
	0x07, 0x00, 0x00,
}
