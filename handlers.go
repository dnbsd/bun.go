package bun

import "github.com/nats-io/nats.go"

type HandlerFunc func(*Context) error

type ConnHandlerFunc = nats.ConnHandler

type ConnErrHandlerFunc = nats.ConnErrHandler

type UserJWTHandlerFunc = nats.UserJWTHandler

type SignatureHandlerFunc = nats.SignatureHandler

type ErrorHandlerFunc func(error, *Context)
