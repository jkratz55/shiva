package shiva

// A Handler handles and processes message from Kafka.
//
// When the Consumer receives a message from Kafka it invokes the Handler to handle
// and process the Message. Implementations of Handler should perform any business
// rules and logic required for the given Message.
//
// If the Handler fails to process a Message, it can return a non-nil error value
// to signal to the Consumer it failed to process the Message. The Consumer will
// then invoke the DeadLetterHandler if one is configured. A nil error value will
// always be interpreted as the Handler successfully processed the Message.
type Handler interface {
	Handle(msg Message) error
}

// A HandlerFunc type is an adapter to allow the use of ordinary functions as a
// Handler.
type HandlerFunc func(msg Message) error

func (f HandlerFunc) Handle(msg Message) error {
	return f(msg)
}

// Middleware is a function type that wraps a Handler to enable intercepting
// a Handler.
type Middleware func(Handler) Handler

// ChainMiddlewares chains multiple Middleware functions into a single Middleware by
// applying them in reverse order.
func ChainMiddlewares(hs ...Middleware) Middleware {
	return func(next Handler) Handler {
		for i := len(hs) - 1; i >= 0; i-- {
			next = hs[i](next)
		}
		return next
	}
}

// WrapHandler applies a chain of Middleware to a Handler, wrapping it in the order provided.
func WrapHandler(h Handler, mw ...Middleware) Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}
