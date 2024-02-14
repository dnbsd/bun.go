package nats_router

import (
	"encoding/json"
	"github.com/nats-io/nats.go"
)

type Context struct {
	msg *nats.Msg
}

func (c *Context) Message(msg *nats.Msg) error {
	return c.msg.RespondMsg(msg)
}

func (c *Context) Blob(b []byte) error {
	return c.msg.Respond(b)
}

func (c *Context) String(s string) error {
	return c.msg.Respond([]byte(s))
}

func (c *Context) JSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.msg.Respond(b)
}
