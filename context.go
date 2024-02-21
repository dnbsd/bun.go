package bun

import (
	"encoding/json"
	"github.com/nats-io/nats.go"
)

type Context struct {
	msg *nats.Msg
}

func (c *Context) BindJSON(v any) error {
	return json.Unmarshal(c.msg.Data, v)
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

func (c *Context) Error(err error) error {
	return c.msg.Respond([]byte(err.Error()))
}

func (c *Context) JSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.msg.Respond(b)
}
