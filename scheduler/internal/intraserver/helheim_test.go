// This file was generated by github.com/nelsam/hel.  Do not
// edit this code by hand unless you *really* know what you're
// doing.  Expect any changes made manually to be overwritten
// the next time hel regenerates this file.

package intraserver_test

import "github.com/apoydence/talaria/scheduler/internal/server"

type mockNodeFetcher struct {
	FetchAllNodesCalled chan bool
	FetchAllNodesOutput struct {
		Ret0 chan []server.NodeInfo
	}
}

func newMockNodeFetcher() *mockNodeFetcher {
	m := &mockNodeFetcher{}
	m.FetchAllNodesCalled = make(chan bool, 100)
	m.FetchAllNodesOutput.Ret0 = make(chan []server.NodeInfo, 100)
	return m
}
func (m *mockNodeFetcher) FetchAllNodes() []server.NodeInfo {
	m.FetchAllNodesCalled <- true
	return <-m.FetchAllNodesOutput.Ret0
}