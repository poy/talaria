// doing.  Expect any changes made manually to be overwritten
// the next time hel regenerates this file.

package end2end_test

import (
	"golang.org/x/net/context"

	"github.com/poy/talaria/api/intra"
)

type mockIntraServer struct {
	CreateCalled chan bool
	CreateInput  struct {
		Ctx chan context.Context
		In  chan *intra.CreateInfo
	}
	CreateOutput struct {
		Ret0 chan *intra.CreateResponse
		Ret1 chan error
	}
	LeaderCalled chan bool
	LeaderInput  struct {
		Ctx chan context.Context
		In  chan *intra.LeaderRequest
	}
	LeaderOutput struct {
		Ret0 chan *intra.LeaderResponse
		Ret1 chan error
	}
	StatusCalled chan bool
	StatusInput  struct {
		Ctx chan context.Context
		In  chan *intra.StatusRequest
	}
	StatusOutput struct {
		Ret0 chan *intra.StatusResponse
		Ret1 chan error
	}
	UpdateConfigCalled chan bool
	UpdateConfigInput  struct {
		Ctx chan context.Context
		In  chan *intra.UpdateConfigRequest
	}
	UpdateConfigOutput struct {
		Ret0 chan *intra.UpdateConfigResponse
		Ret1 chan error
	}
}

func newMockIntraServer() *mockIntraServer {
	m := &mockIntraServer{}
	m.CreateCalled = make(chan bool, 100)
	m.CreateInput.Ctx = make(chan context.Context, 100)
	m.CreateInput.In = make(chan *intra.CreateInfo, 100)
	m.CreateOutput.Ret0 = make(chan *intra.CreateResponse, 100)
	m.CreateOutput.Ret1 = make(chan error, 100)
	m.LeaderCalled = make(chan bool, 100)
	m.LeaderInput.Ctx = make(chan context.Context, 100)
	m.LeaderInput.In = make(chan *intra.LeaderRequest, 100)
	m.LeaderOutput.Ret0 = make(chan *intra.LeaderResponse, 100)
	m.LeaderOutput.Ret1 = make(chan error, 100)
	m.StatusCalled = make(chan bool, 100)
	m.StatusInput.Ctx = make(chan context.Context, 100)
	m.StatusInput.In = make(chan *intra.StatusRequest, 100)
	m.StatusOutput.Ret0 = make(chan *intra.StatusResponse, 100)
	m.StatusOutput.Ret1 = make(chan error, 100)
	m.UpdateConfigCalled = make(chan bool, 100)
	m.UpdateConfigInput.Ctx = make(chan context.Context, 100)
	m.UpdateConfigInput.In = make(chan *intra.UpdateConfigRequest, 100)
	m.UpdateConfigOutput.Ret0 = make(chan *intra.UpdateConfigResponse, 100)
	m.UpdateConfigOutput.Ret1 = make(chan error, 100)
	return m
}
func (m *mockIntraServer) Create(ctx context.Context, in *intra.CreateInfo) (*intra.CreateResponse, error) {
	m.CreateCalled <- true
	m.CreateInput.Ctx <- ctx
	m.CreateInput.In <- in
	return <-m.CreateOutput.Ret0, <-m.CreateOutput.Ret1
}
func (m *mockIntraServer) Leader(ctx context.Context, in *intra.LeaderRequest) (*intra.LeaderResponse, error) {
	m.LeaderCalled <- true
	m.LeaderInput.Ctx <- ctx
	m.LeaderInput.In <- in
	return <-m.LeaderOutput.Ret0, <-m.LeaderOutput.Ret1
}
func (m *mockIntraServer) Status(ctx context.Context, in *intra.StatusRequest) (*intra.StatusResponse, error) {
	m.StatusCalled <- true
	m.StatusInput.Ctx <- ctx
	m.StatusInput.In <- in
	return <-m.StatusOutput.Ret0, <-m.StatusOutput.Ret1
}
func (m *mockIntraServer) UpdateConfig(ctx context.Context, in *intra.UpdateConfigRequest) (*intra.UpdateConfigResponse, error) {
	m.UpdateConfigCalled <- true
	m.UpdateConfigInput.Ctx <- ctx
	m.UpdateConfigInput.In <- in
	return <-m.UpdateConfigOutput.Ret0, <-m.UpdateConfigOutput.Ret1
}
