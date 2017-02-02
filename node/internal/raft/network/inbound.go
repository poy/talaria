package network

import (
	"fmt"
	"log"
	"net"
	"sync"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/api/intra"
	rafthashi "github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type Inbound struct {
	addr             string
	schedulerHandler intra.NodeServer

	lock      sync.Mutex
	consumers map[string]chan rafthashi.RPC
}

func NewInbound(addr string, schedulerHandler intra.NodeServer) *Inbound {
	i := &Inbound{
		addr:             addr,
		schedulerHandler: schedulerHandler,
		consumers:        make(map[string]chan rafthashi.RPC),
	}
	i.addr = i.runServer()

	return i
}

func (i *Inbound) Addr() string {
	return i.addr
}

func (i *Inbound) Fetch(bufferName string) <-chan rafthashi.RPC {
	i.lock.Lock()
	defer i.lock.Unlock()

	c, ok := i.consumers[bufferName]
	if !ok {
		c = make(chan rafthashi.RPC, 1)
		i.consumers[bufferName] = c
	}

	return c
}

func (i *Inbound) AppendEntries(ctx context.Context, in *intra.AppendEntriesRequest) (resp *intra.AppendEntriesResponse, err error) {
	i.lock.Lock()
	c := i.consumers[in.BufferName]
	i.lock.Unlock()

	if c == nil {
		return nil, fmt.Errorf("unknown buffer: %s", in.BufferName)
	}

	rpc, respChan := i.convertAppendToRPC(in)

	select {
	case c <- rpc:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case resp := <-respChan:
		return i.convertRPCRespToAppendResp(resp.Response.(*rafthashi.AppendEntriesResponse)), resp.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (i *Inbound) RequestVote(ctx context.Context, in *intra.RequestVoteRequest) (resp *intra.RequestVoteResponse, err error) {
	i.lock.Lock()
	c := i.consumers[in.BufferName]
	i.lock.Unlock()

	if c == nil {
		return nil, fmt.Errorf("unknown buffer: %s", in.BufferName)
	}

	rpc, respChan := i.convertRequestVoteToRPC(in)

	select {
	case c <- rpc:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case resp := <-respChan:
		return i.convertRPCRespToVoteResp(resp.Response.(*rafthashi.RequestVoteResponse)), resp.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (i *Inbound) runServer() string {
	lis, err := net.Listen("tcp4", i.addr)
	if err != nil {
		log.Fatalf("Failed to start intra server: %s", err)
	}
	log.Printf("Listening for intra communication on %s", lis.Addr())

	s := grpc.NewServer()
	intra.RegisterNodeRaftServer(s, i)
	intra.RegisterNodeServer(s, i.schedulerHandler)

	go func() {
		if err = s.Serve(lis); err != nil {
			log.Fatalf("Failed to start intra server: %s", err)
		}
	}()

	return lis.Addr().String()
}

func (i *Inbound) convertAppendToRPC(req *intra.AppendEntriesRequest) (rafthashi.RPC, <-chan rafthashi.RPCResponse) {
	command := &rafthashi.AppendEntriesRequest{
		Term:              req.Term,
		Leader:            req.Leader,
		PrevLogEntry:      req.PrevLogEntry,
		PrevLogTerm:       req.PrevLogTerm,
		LeaderCommitIndex: req.LeaderCommitIndex,
	}

	for _, entry := range req.Entries {
		command.Entries = append(command.Entries, &rafthashi.Log{
			Index: entry.Index,
			Term:  entry.Term,
			Type:  rafthashi.LogType(entry.Type),
			Data:  entry.Data,
		})
	}

	respChan := make(chan rafthashi.RPCResponse, 1)

	return rafthashi.RPC{
		Command:  command,
		RespChan: respChan,
	}, respChan
}

func (i *Inbound) convertRPCRespToAppendResp(rpc *rafthashi.AppendEntriesResponse) *intra.AppendEntriesResponse {
	if rpc == nil {
		return new(intra.AppendEntriesResponse)
	}

	return &intra.AppendEntriesResponse{
		LastLog:        rpc.LastLog,
		Success:        rpc.Success,
		NoRetryBackoff: rpc.NoRetryBackoff,
	}
}

func (i *Inbound) convertRequestVoteToRPC(req *intra.RequestVoteRequest) (rafthashi.RPC, <-chan rafthashi.RPCResponse) {
	command := &rafthashi.RequestVoteRequest{
		Term:         req.Term,
		Candidate:    req.Candidate,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	respChan := make(chan rafthashi.RPCResponse, 1)

	return rafthashi.RPC{
		Command:  command,
		RespChan: respChan,
	}, respChan
}

func (i *Inbound) convertRPCRespToVoteResp(rpc *rafthashi.RequestVoteResponse) *intra.RequestVoteResponse {
	if rpc == nil {
		return new(intra.RequestVoteResponse)
	}

	return &intra.RequestVoteResponse{
		Term:    rpc.Term,
		Peers:   rpc.Peers,
		Granted: rpc.Granted,
	}
}
