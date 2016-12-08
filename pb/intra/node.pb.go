// Code generated by protoc-gen-go.
// source: node.proto
// DO NOT EDIT!

/*
Package intra is a generated protocol buffer package.

It is generated from these files:
	node.proto
	scheduler.proto

It has these top-level messages:
	CreateInfo
	CreateResponse
	PeerInfo
	LeaderRequest
	LeaderInfo
	UpdateMessage
	UpdateResponse
	UpdateConfigRequest
	UpdateConfigResponse
	StatusRequest
	StatusResponse
	FromIdRequest
	FromIdResponse
*/
package intra

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import raftpb "github.com/coreos/etcd/raft/raftpb"

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

type UpdateResponse_Code int32

const (
	UpdateResponse_Success       UpdateResponse_Code = 0
	UpdateResponse_InvalidID     UpdateResponse_Code = 1
	UpdateResponse_InvalidBuffer UpdateResponse_Code = 2
	UpdateResponse_RetryFailure  UpdateResponse_Code = 3
)

var UpdateResponse_Code_name = map[int32]string{
	0: "Success",
	1: "InvalidID",
	2: "InvalidBuffer",
	3: "RetryFailure",
}
var UpdateResponse_Code_value = map[string]int32{
	"Success":       0,
	"InvalidID":     1,
	"InvalidBuffer": 2,
	"RetryFailure":  3,
}

func (x UpdateResponse_Code) String() string {
	return proto.EnumName(UpdateResponse_Code_name, int32(x))
}
func (UpdateResponse_Code) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{6, 0} }

type CreateInfo struct {
	Name  string      `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Id    uint64      `protobuf:"varint,2,opt,name=id" json:"id,omitempty"`
	Peers []*PeerInfo `protobuf:"bytes,3,rep,name=peers" json:"peers,omitempty"`
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
	Id uint64 `protobuf:"varint,1,opt,name=id" json:"id,omitempty"`
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

type LeaderInfo struct {
	Id uint64 `protobuf:"varint,1,opt,name=id" json:"id,omitempty"`
}

func (m *LeaderInfo) Reset()                    { *m = LeaderInfo{} }
func (m *LeaderInfo) String() string            { return proto.CompactTextString(m) }
func (*LeaderInfo) ProtoMessage()               {}
func (*LeaderInfo) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

type UpdateMessage struct {
	Name     string            `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Messages []*raftpb.Message `protobuf:"bytes,2,rep,name=messages" json:"messages,omitempty"`
}

func (m *UpdateMessage) Reset()                    { *m = UpdateMessage{} }
func (m *UpdateMessage) String() string            { return proto.CompactTextString(m) }
func (*UpdateMessage) ProtoMessage()               {}
func (*UpdateMessage) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

func (m *UpdateMessage) GetMessages() []*raftpb.Message {
	if m != nil {
		return m.Messages
	}
	return nil
}

type UpdateResponse struct {
	Code UpdateResponse_Code `protobuf:"varint,1,opt,name=code,enum=intra.UpdateResponse_Code" json:"code,omitempty"`
}

func (m *UpdateResponse) Reset()                    { *m = UpdateResponse{} }
func (m *UpdateResponse) String() string            { return proto.CompactTextString(m) }
func (*UpdateResponse) ProtoMessage()               {}
func (*UpdateResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{6} }

type UpdateConfigRequest struct {
	Name   string             `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Change *raftpb.ConfChange `protobuf:"bytes,2,opt,name=change" json:"change,omitempty"`
}

func (m *UpdateConfigRequest) Reset()                    { *m = UpdateConfigRequest{} }
func (m *UpdateConfigRequest) String() string            { return proto.CompactTextString(m) }
func (*UpdateConfigRequest) ProtoMessage()               {}
func (*UpdateConfigRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{7} }

func (m *UpdateConfigRequest) GetChange() *raftpb.ConfChange {
	if m != nil {
		return m.Change
	}
	return nil
}

type UpdateConfigResponse struct {
}

func (m *UpdateConfigResponse) Reset()                    { *m = UpdateConfigResponse{} }
func (m *UpdateConfigResponse) String() string            { return proto.CompactTextString(m) }
func (*UpdateConfigResponse) ProtoMessage()               {}
func (*UpdateConfigResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{8} }

type StatusRequest struct {
}

func (m *StatusRequest) Reset()                    { *m = StatusRequest{} }
func (m *StatusRequest) String() string            { return proto.CompactTextString(m) }
func (*StatusRequest) ProtoMessage()               {}
func (*StatusRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{9} }

type StatusResponse struct {
	Id uint64 `protobuf:"varint,1,opt,name=id" json:"id,omitempty"`
}

func (m *StatusResponse) Reset()                    { *m = StatusResponse{} }
func (m *StatusResponse) String() string            { return proto.CompactTextString(m) }
func (*StatusResponse) ProtoMessage()               {}
func (*StatusResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{10} }

func init() {
	proto.RegisterType((*CreateInfo)(nil), "intra.CreateInfo")
	proto.RegisterType((*CreateResponse)(nil), "intra.CreateResponse")
	proto.RegisterType((*PeerInfo)(nil), "intra.PeerInfo")
	proto.RegisterType((*LeaderRequest)(nil), "intra.LeaderRequest")
	proto.RegisterType((*LeaderInfo)(nil), "intra.LeaderInfo")
	proto.RegisterType((*UpdateMessage)(nil), "intra.UpdateMessage")
	proto.RegisterType((*UpdateResponse)(nil), "intra.UpdateResponse")
	proto.RegisterType((*UpdateConfigRequest)(nil), "intra.UpdateConfigRequest")
	proto.RegisterType((*UpdateConfigResponse)(nil), "intra.UpdateConfigResponse")
	proto.RegisterType((*StatusRequest)(nil), "intra.StatusRequest")
	proto.RegisterType((*StatusResponse)(nil), "intra.StatusResponse")
	proto.RegisterEnum("intra.UpdateResponse_Code", UpdateResponse_Code_name, UpdateResponse_Code_value)
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
	Leader(ctx context.Context, in *LeaderRequest, opts ...grpc.CallOption) (*LeaderInfo, error)
	Update(ctx context.Context, in *UpdateMessage, opts ...grpc.CallOption) (*UpdateResponse, error)
	UpdateConfig(ctx context.Context, in *UpdateConfigRequest, opts ...grpc.CallOption) (*UpdateConfigResponse, error)
	Status(ctx context.Context, in *StatusRequest, opts ...grpc.CallOption) (*StatusResponse, error)
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

func (c *nodeClient) Leader(ctx context.Context, in *LeaderRequest, opts ...grpc.CallOption) (*LeaderInfo, error) {
	out := new(LeaderInfo)
	err := grpc.Invoke(ctx, "/intra.Node/Leader", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeClient) Update(ctx context.Context, in *UpdateMessage, opts ...grpc.CallOption) (*UpdateResponse, error) {
	out := new(UpdateResponse)
	err := grpc.Invoke(ctx, "/intra.Node/Update", in, out, c.cc, opts...)
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

// Server API for Node service

type NodeServer interface {
	Create(context.Context, *CreateInfo) (*CreateResponse, error)
	Leader(context.Context, *LeaderRequest) (*LeaderInfo, error)
	Update(context.Context, *UpdateMessage) (*UpdateResponse, error)
	UpdateConfig(context.Context, *UpdateConfigRequest) (*UpdateConfigResponse, error)
	Status(context.Context, *StatusRequest) (*StatusResponse, error)
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

func _Node_Update_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateMessage)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).Update(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/intra.Node/Update",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).Update(ctx, req.(*UpdateMessage))
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

var _Node_serviceDesc = grpc.ServiceDesc{
	ServiceName: "intra.Node",
	HandlerType: (*NodeServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Create",
			Handler:    _Node_Create_Handler,
		},
		{
			MethodName: "Leader",
			Handler:    _Node_Leader_Handler,
		},
		{
			MethodName: "Update",
			Handler:    _Node_Update_Handler,
		},
		{
			MethodName: "UpdateConfig",
			Handler:    _Node_UpdateConfig_Handler,
		},
		{
			MethodName: "Status",
			Handler:    _Node_Status_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: fileDescriptor0,
}

func init() { proto.RegisterFile("node.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 476 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x7c, 0x53, 0x51, 0x8b, 0xd3, 0x40,
	0x10, 0x4e, 0xd2, 0x5c, 0xbc, 0x9b, 0x5e, 0x72, 0xe9, 0x7a, 0x4a, 0x89, 0x3e, 0x94, 0x15, 0xa1,
	0x28, 0xa6, 0xd0, 0x13, 0xee, 0xdd, 0x8a, 0x52, 0x50, 0x39, 0x72, 0x1c, 0x3e, 0x6f, 0x93, 0x49,
	0x2f, 0x70, 0xcd, 0xc6, 0xdd, 0x8d, 0xe0, 0x4f, 0xf0, 0xcf, 0xf8, 0x1b, 0x25, 0xd9, 0x4d, 0x74,
	0xa5, 0xf8, 0x92, 0x64, 0x77, 0xbe, 0xf9, 0x66, 0xe6, 0xfb, 0x26, 0x00, 0x35, 0x2f, 0x30, 0x6d,
	0x04, 0x57, 0x9c, 0x9c, 0x54, 0xb5, 0x12, 0x2c, 0x79, 0xb3, 0xaf, 0xd4, 0x7d, 0xbb, 0x4b, 0x73,
	0x7e, 0x58, 0xe5, 0x5c, 0x20, 0x97, 0x2b, 0x54, 0x79, 0xb1, 0x12, 0xac, 0x54, 0xfd, 0xa3, 0xd9,
	0xf5, 0x2f, 0x9d, 0x45, 0xbf, 0x02, 0x6c, 0x04, 0x32, 0x85, 0xdb, 0xba, 0xe4, 0x84, 0x80, 0x5f,
	0xb3, 0x03, 0xce, 0xdd, 0x85, 0xbb, 0x3c, 0xcb, 0xfa, 0x6f, 0x12, 0x81, 0x57, 0x15, 0x73, 0x6f,
	0xe1, 0x2e, 0xfd, 0xcc, 0xab, 0x0a, 0xf2, 0x12, 0x4e, 0x1a, 0x44, 0x21, 0xe7, 0x93, 0xc5, 0x64,
	0x39, 0x5d, 0x5f, 0xa4, 0x7d, 0xdd, 0xf4, 0x06, 0x51, 0x74, 0x1c, 0x99, 0x8e, 0xd2, 0x18, 0x22,
	0x4d, 0x9c, 0xa1, 0x6c, 0x78, 0x2d, 0x91, 0x26, 0x70, 0x3a, 0x80, 0x0c, 0xa9, 0x3b, 0x90, 0xd2,
	0x17, 0x10, 0x7e, 0x42, 0x56, 0xa0, 0xc8, 0xf0, 0x5b, 0x8b, 0x52, 0x1d, 0xeb, 0x84, 0x3e, 0x07,
	0xd0, 0xa0, 0xa3, 0x14, 0x37, 0x10, 0xde, 0x35, 0x05, 0x53, 0xf8, 0x19, 0xa5, 0x64, 0x7b, 0x3c,
	0x3a, 0xcc, 0x6b, 0x38, 0x3d, 0xe8, 0xb0, 0x9c, 0x7b, 0xa6, 0x7f, 0x2d, 0x4a, 0x6a, 0xd2, 0xb2,
	0x11, 0x40, 0x7f, 0xba, 0x10, 0x69, 0xca, 0x61, 0x06, 0x92, 0x82, 0x9f, 0xf3, 0x42, 0x73, 0x46,
	0xeb, 0xc4, 0xcc, 0x6e, 0x83, 0xd2, 0x0d, 0x2f, 0x30, 0xeb, 0x71, 0xf4, 0x23, 0xf8, 0xdd, 0x89,
	0x4c, 0xe1, 0xd1, 0x6d, 0x9b, 0xe7, 0x28, 0x65, 0xec, 0x90, 0x10, 0xce, 0xb6, 0xf5, 0x77, 0xf6,
	0x50, 0x15, 0xdb, 0xf7, 0xb1, 0x4b, 0x66, 0x10, 0x9a, 0xe3, 0xbb, 0xb6, 0x2c, 0x51, 0xc4, 0x1e,
	0x89, 0xe1, 0x3c, 0x43, 0x25, 0x7e, 0x7c, 0x60, 0xd5, 0x43, 0x2b, 0x30, 0x9e, 0xd0, 0x3b, 0x78,
	0xac, 0xab, 0x6c, 0x78, 0x5d, 0x56, 0xfb, 0xff, 0xc8, 0x44, 0x5e, 0x41, 0x90, 0xdf, 0xb3, 0x7a,
	0x8f, 0xbd, 0x69, 0xd3, 0x35, 0x19, 0x26, 0xec, 0x52, 0x37, 0x7d, 0x24, 0x33, 0x08, 0xfa, 0x14,
	0x2e, 0x6d, 0x5a, 0xe3, 0xd5, 0x05, 0x84, 0xb7, 0x8a, 0xa9, 0x56, 0x9a, 0x42, 0x74, 0x01, 0xd1,
	0x70, 0x61, 0xa4, 0xf8, 0x47, 0xff, 0xf5, 0x2f, 0x0f, 0xfc, 0x2f, 0xdd, 0xac, 0x6f, 0x21, 0xd0,
	0xce, 0x93, 0x99, 0xd1, 0xe7, 0xcf, 0x86, 0x25, 0x4f, 0xac, 0xab, 0xb1, 0x9e, 0x43, 0xae, 0x20,
	0xd0, 0xe6, 0x92, 0x4b, 0x03, 0xb1, 0x16, 0x22, 0x99, 0x59, 0xb7, 0x1d, 0x17, 0x75, 0xc8, 0x35,
	0x04, 0xba, 0xfd, 0x31, 0xc9, 0x5a, 0x81, 0xb1, 0x9a, 0x6d, 0x10, 0x75, 0xc8, 0x16, 0xce, 0xff,
	0x9e, 0x9b, 0xd8, 0x4e, 0x5a, 0x1a, 0x27, 0xcf, 0x8e, 0xc6, 0x46, 0xaa, 0x6b, 0x08, 0xb4, 0x32,
	0x63, 0x0f, 0x96, 0x72, 0x63, 0x0f, 0xb6, 0x7c, 0xd4, 0xd9, 0x05, 0xfd, 0x1f, 0x78, 0xf5, 0x3b,
	0x00, 0x00, 0xff, 0xff, 0x2b, 0xe6, 0xe9, 0xf6, 0xc5, 0x03, 0x00, 0x00,
}
