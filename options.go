package shiva

// todo: do I really need this? Or should I refactor the options?

type consumerOptions struct {
	dlHandler DeadLetterHandlerFunc
}

func newConsumerOptions() *consumerOptions {
	return &consumerOptions{
		dlHandler: func(msg Message, err error) {},
	}
}

type ConsumerOption func(*consumerOptions)

func WithDeadLetterHandler(dlHandler DeadLetterHandlerFunc) ConsumerOption {
	return func(o *consumerOptions) {
		o.dlHandler = dlHandler
	}
}
