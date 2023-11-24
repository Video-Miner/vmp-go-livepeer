package net

import (
	context "context"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// OrchestratorClient is the client API for Orchestrator service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type OrchestratorClient interface {
	// Called by the broadcaster to request transcoder info from an orchestrator.
	GetOrchestrator(ctx context.Context, in *OrchestratorRequest, opts ...grpc.CallOption) (*OrchestratorInfo, error)
	EndTranscodingSession(ctx context.Context, in *EndTranscodingSessionRequest, opts ...grpc.CallOption) (*EndTranscodingSessionResponse, error)
	Ping(ctx context.Context, in *PingPong, opts ...grpc.CallOption) (*PingPong, error)
}

type orchestratorClient struct {
	cc grpc.ClientConnInterface
}

func NewOrchestratorClient(cc grpc.ClientConnInterface) OrchestratorClient {
	return &orchestratorClient{cc}
}

func (c *orchestratorClient) GetOrchestrator(ctx context.Context, in *OrchestratorRequest, opts ...grpc.CallOption) (*OrchestratorInfo, error) {
	out := new(OrchestratorInfo)
	err := c.cc.Invoke(ctx, "/net.Orchestrator/GetOrchestrator", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *orchestratorClient) EndTranscodingSession(ctx context.Context, in *EndTranscodingSessionRequest, opts ...grpc.CallOption) (*EndTranscodingSessionResponse, error) {
	out := new(EndTranscodingSessionResponse)
	err := c.cc.Invoke(ctx, "/net.Orchestrator/EndTranscodingSession", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *orchestratorClient) Ping(ctx context.Context, in *PingPong, opts ...grpc.CallOption) (*PingPong, error) {
	out := new(PingPong)
	err := c.cc.Invoke(ctx, "/net.Orchestrator/Ping", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// OrchestratorServer is the server API for Orchestrator service.
type OrchestratorServer interface {
	// Called by the broadcaster to request transcoder info from an orchestrator.
	GetOrchestrator(context.Context, *OrchestratorRequest) (*OrchestratorInfo, error)
	EndTranscodingSession(context.Context, *EndTranscodingSessionRequest) (*EndTranscodingSessionResponse, error)
	Ping(context.Context, *PingPong) (*PingPong, error)
}

// UnimplementedOrchestratorServer can be embedded to have forward compatible implementations.
type UnimplementedOrchestratorServer struct {
}

func (*UnimplementedOrchestratorServer) GetOrchestrator(ctx context.Context, req *OrchestratorRequest) (*OrchestratorInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetOrchestrator not implemented")
}
func (*UnimplementedOrchestratorServer) EndTranscodingSession(ctx context.Context, req *EndTranscodingSessionRequest) (*EndTranscodingSessionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EndTranscodingSession not implemented")
}
func (*UnimplementedOrchestratorServer) Ping(ctx context.Context, req *PingPong) (*PingPong, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Ping not implemented")
}

func RegisterOrchestratorServer(s *grpc.Server, srv OrchestratorServer) {
	s.RegisterService(&_Orchestrator_serviceDesc, srv)
}

func _Orchestrator_GetOrchestrator_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OrchestratorRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OrchestratorServer).GetOrchestrator(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/net.Orchestrator/GetOrchestrator",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OrchestratorServer).GetOrchestrator(ctx, req.(*OrchestratorRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Orchestrator_EndTranscodingSession_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EndTranscodingSessionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OrchestratorServer).EndTranscodingSession(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/net.Orchestrator/EndTranscodingSession",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OrchestratorServer).EndTranscodingSession(ctx, req.(*EndTranscodingSessionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Orchestrator_Ping_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PingPong)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OrchestratorServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/net.Orchestrator/Ping",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OrchestratorServer).Ping(ctx, req.(*PingPong))
	}
	return interceptor(ctx, in, info, handler)
}

var _Orchestrator_serviceDesc = grpc.ServiceDesc{
	ServiceName: "net.Orchestrator",
	HandlerType: (*OrchestratorServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetOrchestrator",
			Handler:    _Orchestrator_GetOrchestrator_Handler,
		},
		{
			MethodName: "EndTranscodingSession",
			Handler:    _Orchestrator_EndTranscodingSession_Handler,
		},
		{
			MethodName: "Ping",
			Handler:    _Orchestrator_Ping_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "net/lp_rpc.proto",
}

// TranscoderClient is the client API for Transcoder service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type TranscoderClient interface {
	// Called by the transcoder to register to an orchestrator. The orchestrator
	// notifies registered transcoders of segments as they come in.
	RegisterTranscoder(ctx context.Context, in *RegisterRequest, opts ...grpc.CallOption) (Transcoder_RegisterTranscoderClient, error)
}

type transcoderClient struct {
	cc grpc.ClientConnInterface
}

func NewTranscoderClient(cc grpc.ClientConnInterface) TranscoderClient {
	return &transcoderClient{cc}
}

func (c *transcoderClient) RegisterTranscoder(ctx context.Context, in *RegisterRequest, opts ...grpc.CallOption) (Transcoder_RegisterTranscoderClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Transcoder_serviceDesc.Streams[0], "/net.Transcoder/RegisterTranscoder", opts...)
	if err != nil {
		return nil, err
	}
	x := &transcoderRegisterTranscoderClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Transcoder_RegisterTranscoderClient interface {
	Recv() (*NotifySegment, error)
	grpc.ClientStream
}

type transcoderRegisterTranscoderClient struct {
	grpc.ClientStream
}

func (x *transcoderRegisterTranscoderClient) Recv() (*NotifySegment, error) {
	m := new(NotifySegment)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// TranscoderServer is the server API for Transcoder service.
type TranscoderServer interface {
	// Called by the transcoder to register to an orchestrator. The orchestrator
	// notifies registered transcoders of segments as they come in.
	RegisterTranscoder(*RegisterRequest, Transcoder_RegisterTranscoderServer) error
}

// UnimplementedTranscoderServer can be embedded to have forward compatible implementations.
type UnimplementedTranscoderServer struct {
}

func (*UnimplementedTranscoderServer) RegisterTranscoder(req *RegisterRequest, srv Transcoder_RegisterTranscoderServer) error {
	return status.Errorf(codes.Unimplemented, "method RegisterTranscoder not implemented")
}

func RegisterTranscoderServer(s *grpc.Server, srv TranscoderServer) {
	s.RegisterService(&_Transcoder_serviceDesc, srv)
}

func _Transcoder_RegisterTranscoder_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RegisterRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(TranscoderServer).RegisterTranscoder(m, &transcoderRegisterTranscoderServer{stream})
}

type Transcoder_RegisterTranscoderServer interface {
	Send(*NotifySegment) error
	grpc.ServerStream
}

type transcoderRegisterTranscoderServer struct {
	grpc.ServerStream
}

func (x *transcoderRegisterTranscoderServer) Send(m *NotifySegment) error {
	return x.ServerStream.SendMsg(m)
}

var _Transcoder_serviceDesc = grpc.ServiceDesc{
	ServiceName: "net.Transcoder",
	HandlerType: (*TranscoderServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "RegisterTranscoder",
			Handler:       _Transcoder_RegisterTranscoder_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "net/lp_rpc.proto",
}
