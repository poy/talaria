package raft

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/apoydence/talaria/api/intra"
	"github.com/apoydence/talaria/node/internal/raft/network"
	rafthashi "github.com/hashicorp/raft"
)

type ClientFetcher interface {
	Fetch(target string) intra.NodeRaftClient
}

type transport struct {
	bufferName    string
	intraAddr     string
	clientFetcher ClientFetcher
	rpc           <-chan rafthashi.RPC
	outboundRpc   chan rafthashi.RPC

	lock      sync.Mutex
	heartbeat func(rpc rafthashi.RPC)
}

func NewTransport(bufferName, intraAddr string, clientFetcher ClientFetcher, rpc <-chan rafthashi.RPC) rafthashi.Transport {
	return &transport{
		bufferName:    bufferName,
		clientFetcher: clientFetcher,
		rpc:           rpc,
		intraAddr:     intraAddr,
		outboundRpc:   make(chan rafthashi.RPC, 100),
	}
}

func (t *transport) Consumer() <-chan rafthashi.RPC {
	//TODO: Do this in inbound
	go func() {
		for rpc := range t.rpc {
			ae, ok := rpc.Command.(*rafthashi.AppendEntriesRequest)
			if !ok {
				t.outboundRpc <- rpc
				continue
			}

			t.lock.Lock()
			heartbeat := t.heartbeat
			t.lock.Unlock()

			if ae.Term != 0 && ae.Leader != nil &&
				ae.PrevLogEntry == 0 && ae.PrevLogTerm == 0 &&
				len(ae.Entries) == 0 && ae.LeaderCommitIndex == 0 &&
				heartbeat != nil {
				heartbeat(rpc)
				continue
			}

			t.outboundRpc <- rpc
		}
	}()

	return t.outboundRpc
}

func (t *transport) LocalAddr() string {
	return t.intraAddr
}

func (t *transport) AppendEntriesPipeline(target string) (rafthashi.AppendPipeline, error) {
	client := t.clientFetcher.Fetch(target)
	pipeline := network.NewPipeline(t.bufferName, target, client)

	return pipeline, nil
}

func (t *transport) AppendEntries(target string, args *rafthashi.AppendEntriesRequest, resp *rafthashi.AppendEntriesResponse) error {
	client := t.clientFetcher.Fetch(target)
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))

	req := network.ConvertToAppendRequest(t.bufferName, target, args)
	r, err := client.AppendEntries(ctx, req)

	if err != nil {
		return err
	}

	network.SetRPCFromAppendResp(r, resp)
	return nil
}

func (t *transport) RequestVote(target string, args *rafthashi.RequestVoteRequest, resp *rafthashi.RequestVoteResponse) error {
	client := t.clientFetcher.Fetch(target)
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))

	req := t.convertToVoteRequest(target, args)
	r, err := client.RequestVote(ctx, req)

	if err != nil {
		return err
	}

	t.setRPCFromVoteResp(r, resp)
	return nil
}

func (t *transport) InstallSnapshot(target string, args *rafthashi.InstallSnapshotRequest, resp *rafthashi.InstallSnapshotResponse, data io.Reader) error {
	panic("not implemented")
	return nil
}

func (t *transport) EncodePeer(peer string) []byte {
	return []byte(peer)
}

func (t *transport) DecodePeer(peer []byte) string {
	return string(peer)
}

func (t *transport) SetHeartbeatHandler(cb func(rpc rafthashi.RPC)) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.heartbeat = cb
}

func (t *transport) convertToVoteRequest(target string, args *rafthashi.RequestVoteRequest) *intra.RequestVoteRequest {
	req := &intra.RequestVoteRequest{
		BufferName:   t.bufferName,
		Term:         args.Term,
		Candidate:    args.Candidate,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	}

	return req
}

func (t *transport) setRPCFromVoteResp(resp *intra.RequestVoteResponse, rpc *rafthashi.RequestVoteResponse) {
	rpc.Term = resp.Term
	rpc.Peers = resp.Peers
	rpc.Granted = resp.Granted
}
