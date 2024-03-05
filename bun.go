package bun

import (
	"context"
	"github.com/nats-io/nats.go"
	"sync"
	"time"
)

type Arguments struct {
	Name                 string
	Servers              []string
	NoRandomize          bool
	MaxReconnect         int
	ReconnectWait        time.Duration
	Timeout              time.Duration
	DrainTimeout         time.Duration
	FlusherTimeout       time.Duration
	PingInterval         time.Duration
	MaxPingsOut          int
	User                 string
	Password             string
	Token                string
	Compression          bool
	IgnoreAuthErrorAbort bool
	SkipHostLookup       bool
	MsgChanCapacity      int
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
	UserJWTHandler      UserJWTHandlerFunc
	SignatureHandler    SignatureHandlerFunc
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
		ConnectedCB:          s.ConnectedHandler,
		ReconnectedCB:        s.ReconnectedHandler,
		DisconnectedErrCB:    s.DisconnectedHandler,
		UserJWT:              s.UserJWTHandler,
		SignatureCB:          s.SignatureHandler,
	}

	nc, err := opts.Connect()
	if err != nil {
		return err
	}
	defer nc.Close()

	var (
		wg          sync.WaitGroup
		workers     []*worker
		schedulerCh = make(chan *worker, 128)
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
			workers = append(workers, w)
			select {
			case schedulerCh <- w:
			case <-ctx.Done():
				break loop
			}

		case worker := <-schedulerCh:
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = worker.Start(context.Background())
			}()

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
