package nats_router

import (
	"context"
	"github.com/nats-io/nats.go"
	"sync"
	"time"
)

type Arguments struct {
	Name           string
	Servers        []string
	NoRandomize    bool
	MaxReconnect   int
	ReconnectWait  time.Duration
	Timeout        time.Duration
	DrainTimeout   time.Duration
	FlusherTimeout time.Duration
	PingInterval   time.Duration
	MaxPingsOut    int
	// TODO: rename!
	SubChanLen           int
	User                 string
	Password             string
	Token                string
	Compression          bool
	IgnoreAuthErrorAbort bool
	SkipHostLookup       bool
}

type subscribe struct {
	Subject  string
	Group    string
	Handlers []HandlerFunc
}

type Conn struct {
	args                Arguments
	subscribeCh         chan subscribe
	ConnectedHandler    ConnHandlerFunc
	ReconnectedHandler  ConnHandlerFunc
	DisconnectedHandler ConnErrHandlerFunc
	ErrorHandler        ErrorHandlerFunc
}

func New(args Arguments) *Conn {
	return &Conn{
		args:        args,
		subscribeCh: make(chan subscribe, 255),
	}
}

func (s *Conn) Start(ctx context.Context) error {
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
			msgCh := make(chan *nats.Msg, s.args.SubChanLen)
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
			go func() {
				wg.Add(1)
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

func (s *Conn) Subscribe(subject, group string, handlers ...HandlerFunc) {
	s.subscribeCh <- subscribe{
		Subject:  subject,
		Group:    group,
		Handlers: handlers,
	}
}
