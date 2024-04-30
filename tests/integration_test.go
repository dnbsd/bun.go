package tests

import (
	"context"
	"errors"
	"github.com/dnbsd/bun.go"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRequestReply(t *testing.T) {
	_, nc := newNatsServerAndConnection(t)
	b := bun.New(bun.Arguments{
		Servers: []string{
			"localhost:14444",
		},
	})
	b.Subscribe("test.string", func(c *bun.Context) error {
		return c.String("string!")
	})
	b.Subscribe("test.blob", func(c *bun.Context) error {
		return c.Blob([]byte("blob!"))
	})
	b.Subscribe("test.json", func(c *bun.Context) error {
		return c.JSON(map[string]any{
			"key": "value",
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := b.Start(ctx)
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

func TestGroupRequestReply(t *testing.T) {
	respCh := make(chan string, 2)
	_, nc := newNatsServerAndConnection(t)
	b1 := bun.New(bun.Arguments{
		Servers: []string{
			"localhost:14444",
		},
	})
	b1.SubscribeGroup("test.string", "testers", func(c *bun.Context) error {
		respCh <- "b1"
		return c.String("string!")
	})
	b2 := bun.New(bun.Arguments{
		Servers: []string{
			"localhost:14444",
		},
	})
	b2.SubscribeGroup("test.string", "testers", func(c *bun.Context) error {
		respCh <- "b2"
		return c.String("string!")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := b1.Start(ctx)
		assert.NoError(t, err)
	}()
	go func() {
		err := b2.Start(ctx)
		assert.NoError(t, err)
	}()

	time.Sleep(1 * time.Second)

	{
		resp, err := nc.Request("test.string", []byte("what are you?!"), 2*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, "string!", string(resp.Data))
		assert.Len(t, respCh, 1)
	}
}

func TestErrorHandler(t *testing.T) {
	_, nc := newNatsServerAndConnection(t)
	var customError = errors.New("expected error")
	s := bun.New(bun.Arguments{
		Servers: []string{
			"localhost:14444",
		},
	})
	s.ErrorHandler = func(err error, c *bun.Context) {
		assert.ErrorIs(t, err, customError)
		_ = c.String(err.Error())
	}
	s.Subscribe("test.error",
		func(c *bun.Context) error {
			return customError
		}, func(c *bun.Context) error {
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
	_, nc := newNatsServerAndConnection(t)
	var customError = errors.New("expected error")
	s := bun.New(bun.Arguments{
		Servers: []string{
			"localhost:14444",
		},
	})
	s.Subscribe("test.error", func(c *bun.Context) error {
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
	_, nc := newNatsServerAndConnection(t)
	s := bun.New(bun.Arguments{
		Servers: []string{
			"localhost:14444",
		},
	})
	s.Subscribe("test.bind", func(c *bun.Context) error {
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

func TestStatusHandler(t *testing.T) {
	srv1 := newNatsServer(14444)
	defer srv1.Shutdown()
	srv2 := newNatsServer(14445)
	defer srv2.Shutdown()
	statusCh := make(chan string, 3)
	s := bun.New(bun.Arguments{
		Servers: []string{
			"localhost:14444",
			"localhost:14445",
		},
		NoRandomize: true,
	})
	s.ConnectedHandler = func(nc *nats.Conn) {
		statusCh <- "connected"
	}
	s.ReconnectedHandler = func(nc *nats.Conn) {
		statusCh <- "reconnected"
	}
	s.DisconnectedHandler = func(nc *nats.Conn, err error) {
		statusCh <- "disconnected"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := s.Start(ctx)
		assert.NoError(t, err)
	}()

	time.Sleep(1 * time.Second)

	{
		status := <-statusCh
		assert.Equal(t, "connected", status)
	}

	{
		srv1.Shutdown()
		status := <-statusCh
		assert.Equal(t, "disconnected", status)
	}

	{
		status := <-statusCh
		assert.Equal(t, "reconnected", status)
	}
}

func TestContextClose(t *testing.T) {
	_, nc := newNatsServerAndConnection(t)
	s := bun.New(bun.Arguments{
		Servers: []string{
			"localhost:14444",
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Subscribe("test.context", func(c *bun.Context) error {
		cancel()
		var resp string
		select {
		case <-time.After(2 * time.Second):
			resp = "timeout"
		case <-c.Context().Done():
			resp = "ok"
		}
		return c.String(resp)
	})

	go func() {
		err := s.Start(ctx)
		assert.NoError(t, err)
	}()

	time.Sleep(1 * time.Second)

	{
		resp, err := nc.Request("test.context", []byte(""), 5*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, "ok", string(resp.Data))
	}
}

func newNatsServer(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.NoLog = false
	opts.Port = port
	return natsserver.RunServer(&opts)
}

func newNatsServerAndConnection(t *testing.T) (*server.Server, *nats.Conn) {
	s := newNatsServer(14444)
	nc, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	t.Cleanup(func() {
		nc.Close()
		s.Shutdown()
	})
	return s, nc
}
