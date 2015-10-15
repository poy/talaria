package broker

type controllerProvider func() Controller

func newControllerProvider(leadersOnly bool, ioProvider IoProvider, orchestrator Orchestrator) controllerProvider {
	return func() Controller {
		return NewFileController(leadersOnly, ioProvider, orchestrator)
	}
}

func (c controllerProvider) Provide() Controller {
	return c()
}
