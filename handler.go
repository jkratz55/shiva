package shiva

// Handler
type Handler interface {
	Handle(msg Message) error
}

type HandlerFunc func(msg Message) error

func (f HandlerFunc) Handle(msg Message) error {
	return f(msg)
}

type Middleware func(Handler) Handler

func ChainMiddlewares(hs ...Middleware) Middleware {
	return func(next Handler) Handler {
		for i := len(hs) - 1; i >= 0; i-- {
			next = hs[i](next)
		}
		return next
	}
}
