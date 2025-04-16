package shiva

// A DeadLetterHandler handles messages that could not be successfully processed
// by a Consumer.
//
// DeadLetterHandler is invoked by the Consumer when the Handler returns an error
// from processing a Message from Kafka. Implementations of DeadLetterHandler should
// perform any actions required before the Consumer proceeds to the next Message. This
// can include logging, instrumentation, persisting the message to a database, or
// publishing the message to a dead letter topic to retry later.
//
// If the DeadLetterHandler encounters an error handling an unprocessable message it
// should handle that error if possible and provide any diagnostics such as logging
// or instrumentation. Handle does not return an error as it is meaningless to the
// Consumer as it will move on the next event regardless.
type DeadLetterHandler interface {
	Handle(msg Message, err error)
}

// The DeadLetterHandlerFunc type is an adapter to allow the use of ordinary functions
// as a DeadLetterHandler
type DeadLetterHandlerFunc func(msg Message, err error)

func (d DeadLetterHandlerFunc) Handle(msg Message, err error) {
	d(msg, err)
}
