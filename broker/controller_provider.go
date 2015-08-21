package broker

type controllerProvider func() Controller

func newControllerProvider(ioProvider IoProvider) controllerProvider {
	return func() Controller {
		return NewFileController(ioProvider)
	}
}

func (c controllerProvider) Provide() Controller {
	return c()
}
