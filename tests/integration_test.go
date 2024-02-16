package tests

import (
	"context"
	"errors"
	"fmt"
	"foo"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRequestReply(t *testing.T) {
	nc := newNatsServerAndConnection(t)
	s := nats_router.New(nats_router.Arguments{
		Servers: []string{
			"localhost:14444",
		},
	})
	s.Subscribe("test.string", "", func(c *nats_router.Context) error {
		return c.String("string!")
	})
	s.Subscribe("test.blob", "", func(c *nats_router.Context) error {
		return c.Blob([]byte("blob!"))
	})
	s.Subscribe("test.json", "", func(c *nats_router.Context) error {
		return c.JSON(map[string]any{
			"key": "value",
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := s.Start(ctx)
		assert.NoError(t, err)
	}()

	time.Sleep(1 * time.Second)

	{
		resp, err := nc.Request("test.string", []byte("what are you?!"), 2*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, "string!", string(resp.Data))
	}

	{
		resp, err := nc.Request("test.blob", []byte("what are you?!"), 2*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, []byte("blob!"), resp.Data)
	}

	{
		resp, err := nc.Request("test.json", []byte("what are you?!"), 2*time.Second)
		assert.NoError(t, err)
		assert.JSONEq(t, `{"key":"value"}`, string(resp.Data))
	}
}

func TestErrorHandler(t *testing.T) {
	nc := newNatsServerAndConnection(t)
	var customError = errors.New("expected error")
	s := nats_router.New(nats_router.Arguments{
		Servers: []string{
			"localhost:14444",
		},
	})
	s.ErrorHandler = func(err error, c *nats_router.Context) {
		assert.ErrorIs(t, err, customError)
		_ = c.String(err.Error())
	}
	s.Subscribe("test.error", "",
		func(c *nats_router.Context) error {
			return customError
		}, func(c *nats_router.Context) error {
			assert.NoError(t, errors.New("handler fell through"))
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := s.Start(ctx)
		assert.NoError(t, err)
	}()

	time.Sleep(1 * time.Second)

	{
		resp, err := nc.Request("test.error", []byte("what are you?!"), 2*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, customError.Error(), string(resp.Data))
	}
}

func TestDefaultErrorHandler(t *testing.T) {
	nc := newNatsServerAndConnection(t)
	var customError = errors.New("expected error")
	s := nats_router.New(nats_router.Arguments{
		Servers: []string{
			"localhost:14444",
		},
	})
	s.Subscribe("test.error", "", func(c *nats_router.Context) error {
		err := customError
		_ = c.String(err.Error())
		return err
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := s.Start(ctx)
		assert.NoError(t, err)
	}()

	time.Sleep(1 * time.Second)

	{
		resp, err := nc.Request("test.error", []byte("what are you?!"), 2*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, customError.Error(), string(resp.Data))
	}
}

func TestBindRequestData(t *testing.T) {
	nc := newNatsServerAndConnection(t)
	s := nats_router.New(nats_router.Arguments{
		Servers: []string{
			"localhost:14444",
		},
	})
	s.Subscribe("test.bind", "", func(c *nats_router.Context) error {
		type data struct {
			Name string `json:"name"`
		}
		var expected = data{
			Name: "John Doe",
		}
		var actual data
		err := c.BindJSON(&actual)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
		return c.String("ok")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := s.Start(ctx)
		assert.NoError(t, err)
	}()

	time.Sleep(1 * time.Second)

	{
		resp, err := nc.Request("test.bind", []byte(`{"name":"John Doe"}`), 2*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, "ok", string(resp.Data))
	}
}

func newNatsServerAndConnection(t *testing.T) *nats.Conn {
	opts := natsserver.DefaultTestOptions
	opts.NoLog = false
	opts.Port = 14444
	s := natsserver.RunServer(&opts)
	uri := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(uri)
	assert.NoError(t, err)
	t.Cleanup(func() {
		nc.Close()
		s.Shutdown()
	})
	return nc
}
