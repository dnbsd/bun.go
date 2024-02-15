package nats_router

import (
	"context"
	"github.com/nats-io/nats.go"
)

type worker struct {
	Subscription   *nats.Subscription
	MessageChannel chan *nats.Msg
	Handlers       []HandlerFunc
	ErrorHandler   ErrorHandlerFunc
}

func (w *worker) Start(_ context.Context) error {
	for {
		select {
		case msg, ok := <-w.MessageChannel:
			if !ok {
				return nil
			}

			c := &Context{
				msg: msg,
			}
			for _, handler := range w.Handlers {
				err := handler(c)
				if err != nil {
					if w.ErrorHandler != nil {
						w.ErrorHandler(err, c)
					}
					break
				}
			}
		}
	}
}
