package bun

import (
	"context"
	"github.com/nats-io/nats.go"
	"sync"
	"time"
)

type Arguments struct {
	// Name is an optional name label which will be sent to the server on CONNECT to identify the client.
	Name string
	// Servers is a configured set of servers which this client will use when attempting to connect.
	Servers []string
	// NoRandomize configures whether we will randomize the server pool.
	NoRandomize bool
	// MaxReconnect sets the number of reconnect attempts that will be tried before giving up. If negative,
	// then it will never give up trying to reconnect.
	// Defaults to 60.
	MaxReconnect int
	// ReconnectWait sets the time to backoff after attempting a reconnect to a server that we were already
	// connected to previously.
	// Defaults to 2s.
	ReconnectWait time.Duration
	// Timeout sets the timeout for a Dial operation on a connection.
	// Defaults to 2s.
	Timeout time.Duration
	// DrainTimeout sets the timeout for a Drain Operation to complete.
	// Defaults to 30s.
	DrainTimeout time.Duration
	// FlusherTimeout is the maximum time to wait for write operations to the underlying connection to complete
	// (including the flusher loop).
	// Defaults to 1m.
	FlusherTimeout time.Duration
	// PingInterval is the period at which the client will be sending ping commands to the server disabled if 0 or
	// negative.
	// Defaults to 2m.
	PingInterval time.Duration
	// MaxPingsOut is the maximum number of pending ping commands that can be awaiting a response before raising
	// an ErrStaleConnection error.
	// Defaults to 2.
	MaxPingsOut int
	// User sets the username to be used when connecting to the server.
	User string
	// Password sets the password to be used when connecting to a server.
	Password string
	// Token sets the token to be used when connecting to a server.
	Token string
	// For websocket connections, indicates to the server that the connection
	// supports compression. If the server does too, then data will be compressed.
	Compression bool
	// IgnoreAuthErrorAbort - if set to true, client opts out of the default connect behavior of aborting
	// subsequent reconnect attempts if server returns the same auth error twice (regardless of reconnect policy).
	IgnoreAuthErrorAbort bool
	// SkipHostLookup skips the DNS lookup for the server hostname.
	SkipHostLookup bool
	// MsgChanCapacity is the size of the buffered channel used for ChanQueueSubscribe.
	// Defaults to 65536.
	MsgChanCapacity int
	// UserJWT sets the callback handler that will fetch a user's JWT.
	UserJWTHandler UserJWTHandlerFunc
	// SignatureCB designates the function used to sign the nonce presented from the server.
	SignatureHandler SignatureHandlerFunc
}

type subscribe struct {
	Subject  string
	Group    string
	Handlers []HandlerFunc
}

type Bun struct {
	args                Arguments
	subscribeCh         chan subscribe
	ConnectedHandler    ConnHandlerFunc
	ReconnectedHandler  ConnHandlerFunc
	DisconnectedHandler ConnErrHandlerFunc
	ErrorHandler        ErrorHandlerFunc
}

func New(args Arguments) *Bun {
	return &Bun{
		args:        args,
		subscribeCh: make(chan subscribe, 255),
	}
}

// Start creates a connection to configured NATS servers, registers subscriptions, and starts the workers.
// The call blocks until there is a fatal error or the context is closed.
// Connection is drained before returning, which guarantees delivery of all received message to workers before exit.
func (s *Bun) Start(ctx context.Context) error {
	opts := nats.Options{
		Servers:              s.args.Servers,
		NoRandomize:          s.args.NoRandomize,
		Name:                 s.args.Name,
		AllowReconnect:       true,
		MaxReconnect:         s.args.MaxReconnect,
		ReconnectWait:        s.args.ReconnectWait,
		Timeout:              s.args.Timeout,
		DrainTimeout:         s.args.DrainTimeout,
		FlusherTimeout:       s.args.FlusherTimeout,
		PingInterval:         s.args.PingInterval,
		MaxPingsOut:          s.args.MaxPingsOut,
		User:                 s.args.User,
		Password:             s.args.Password,
		Token:                s.args.Token,
		RetryOnFailedConnect: true,
		Compression:          s.args.Compression,
		IgnoreAuthErrorAbort: s.args.IgnoreAuthErrorAbort,
		SkipHostLookup:       s.args.SkipHostLookup,
		UserJWT:              s.args.UserJWTHandler,
		SignatureCB:          s.args.SignatureHandler,
		ConnectedCB:          s.ConnectedHandler,
		ReconnectedCB:        s.ReconnectedHandler,
		DisconnectedErrCB:    s.DisconnectedHandler,
	}

	nc, err := opts.Connect()
	if err != nil {
		return err
	}
	defer nc.Close()

	var (
		wg      sync.WaitGroup
		workers []*worker
	)

loop:
	for {
		select {
		case req := <-s.subscribeCh:
			msgCh := make(chan *nats.Msg, messageChannelCapacity(s.args.MsgChanCapacity))
			sub, err := nc.ChanQueueSubscribe(req.Subject, req.Group, msgCh)
			if err != nil {
				break loop
			}

			w := &worker{
				Subscription:   sub,
				MessageChannel: msgCh,
				Handlers:       req.Handlers,
				ErrorHandler:   s.ErrorHandler,
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = w.Start(context.Background())
			}()

			workers = append(workers, w)

		case <-ctx.Done():
			break loop
		}
	}

	for i := range workers {
		err := workers[i].Subscription.Drain()
		if err != nil {
			continue
		}

		close(workers[i].MessageChannel)
	}

	wg.Wait()

	return nil
}

// Subscribe subscribes a worker to a subject, passing received messages to registered handlers.
// Handlers are executed sequentially. Handler execution flow is interrupted if a handler returns non-nil error.
// It's safe to call this method after Start.
func (s *Bun) Subscribe(subject string, handlers ...HandlerFunc) {
	if len(handlers) == 0 {
		return
	}
	s.subscribeCh <- subscribe{
		Subject:  subject,
		Handlers: handlers,
	}
}

// SubscribeGroup subscribes a worker to a subject, passing received messages to registered handlers. Workers with the
// same group name will form a queue group and only one member of the group will be selected to receive a message.
// Handlers are executed sequentially. Handler execution flow is interrupted if a handler returns non-nil error.
// It's safe to call this method after Start.
func (s *Bun) SubscribeGroup(subject, group string, handlers ...HandlerFunc) {
	if len(handlers) == 0 {
		return
	}
	s.subscribeCh <- subscribe{
		Subject:  subject,
		Group:    group,
		Handlers: handlers,
	}
}

func messageChannelCapacity(v int) int {
	const defaultValue = 65535
	if v == 0 {
		return defaultValue
	}
	return v
}
