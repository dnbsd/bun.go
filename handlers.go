package nats_router

type HandlerFunc func(*Context) error

type ErrorHandlerFunc func(error, *Context)
