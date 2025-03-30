package shiva

type Handler interface {
	Handle(msg any) error
}

type HandlerFunc func(msg any) error

func (f HandlerFunc) Handle(msg any) error {
	return f(msg)
}

type Middleware func(Handler) Handler
