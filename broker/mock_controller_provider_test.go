package broker_test

import "github.com/apoydence/talaria/broker"

type mockControllerProvider struct {
	controllerCh chan *mockController
}

func newMockControllerProvider() *mockControllerProvider {
	return &mockControllerProvider{
		controllerCh: make(chan *mockController, 100),
	}
}

func (m *mockControllerProvider) Provide() broker.Controller {
	return <-m.controllerCh
}
