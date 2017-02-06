package network

import (
	"context"
	"sync"
	"time"

	"github.com/apoydence/talaria/api/intra"
	rafthashi "github.com/hashicorp/raft"
)

type Pipeline struct {
	client     intra.NodeRaftClient
	doneFuts   chan rafthashi.AppendFuture
	bufferName string
	target     string
}

func NewPipeline(bufferName, target string, client intra.NodeRaftClient) *Pipeline {
	return &Pipeline{
		client:     client,
		bufferName: bufferName,
		target:     target,
		doneFuts:   make(chan rafthashi.AppendFuture, 100),
	}
}

func (p *Pipeline) AppendEntries(args *rafthashi.AppendEntriesRequest, resp *rafthashi.AppendEntriesResponse) (rafthashi.AppendFuture, error) {
	appendReq := ConvertToAppendRequest(p.bufferName, p.target, args)

	fut := &appendFuture{
		start: time.Now(),
		req:   args,
		resp:  resp,
		errs:  make(chan error, 1),
	}

	go func() {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		rxResp, err := p.client.AppendEntries(ctx, appendReq)
		if err == nil {
			SetRPCFromAppendResp(rxResp, fut.resp)
		}

		fut.errs <- err
		p.doneFuts <- fut
	}()
	return fut, nil
}

func (p *Pipeline) Consumer() <-chan rafthashi.AppendFuture {
	return p.doneFuts
}

func (p *Pipeline) Close() error {
	return nil
}

type appendFuture struct {
	req   *rafthashi.AppendEntriesRequest
	resp  *rafthashi.AppendEntriesResponse
	start time.Time

	once sync.Once
	mu   sync.Mutex
	err  error
	errs chan error
}

func (f *appendFuture) Error() error {
	f.once.Do(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.err = <-f.errs
	})

	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

func (f *appendFuture) Start() time.Time {
	return f.start
}

func (f *appendFuture) Request() *rafthashi.AppendEntriesRequest {
	return f.req
}

func (f *appendFuture) Response() *rafthashi.AppendEntriesResponse {
	return f.resp
}

func ConvertToAppendRequest(bufferName, target string, args *rafthashi.AppendEntriesRequest) *intra.AppendEntriesRequest {
	req := &intra.AppendEntriesRequest{
		BufferName:        bufferName,
		Term:              args.Term,
		Leader:            args.Leader,
		PrevLogEntry:      args.PrevLogEntry,
		PrevLogTerm:       args.PrevLogTerm,
		LeaderCommitIndex: args.LeaderCommitIndex,
	}

	for _, entry := range args.Entries {
		req.Entries = append(req.Entries, &intra.RaftLog{
			Index: entry.Index,
			Term:  entry.Term,
			Type:  intra.RaftLog_LogType(entry.Type),
			Data:  entry.Data,
		})
	}

	return req
}

func SetRPCFromAppendResp(resp *intra.AppendEntriesResponse, rpc *rafthashi.AppendEntriesResponse) {
	rpc.Term = resp.Term
	rpc.Success = resp.Success
	rpc.LastLog = resp.LastLog
	rpc.NoRetryBackoff = resp.NoRetryBackoff
}
