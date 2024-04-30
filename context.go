package bun

import (
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go"
)

type Context struct {
	ctx context.Context
	msg *nats.Msg
}

// Context returns the service context.
func (c *Context) Context() context.Context {
	return c.ctx
}

// BindJSON unmarshals data from NATS message into v.
func (c *Context) BindJSON(v any) error {
	return json.Unmarshal(c.msg.Data, v)
}

// Message sends message msg to a Reply topic.
func (c *Context) Message(msg *nats.Msg) error {
	return c.msg.RespondMsg(msg)
}

// Blob sends bytes to a Reply topic.
func (c *Context) Blob(b []byte) error {
	return c.msg.Respond(b)
}

// String sends string to a Reply topic.
func (c *Context) String(s string) error {
	return c.msg.Respond([]byte(s))
}

// Error sends string value of an error to a Reply topic. If error is nil, the method has no effect.
func (c *Context) Error(err error) error {
	if err == nil {
		return nil
	}
	return c.msg.Respond([]byte(err.Error()))
}

// JSON marshals data in v and sends the marshalled data to a Reply topic.
func (c *Context) JSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.msg.Respond(b)
}
