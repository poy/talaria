package broker

type controllerProvider func() Controller

func newControllerProvider(ioProvider IoProvider, orchestrator Orchestrator) controllerProvider {
	return func() Controller {
		return NewFileController(ioProvider, orchestrator)
	}
}

func (c controllerProvider) Provide() Controller {
	return c()
}
