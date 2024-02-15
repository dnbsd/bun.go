package nats_router

import (
	"context"
	"github.com/nats-io/nats.go"
	"sync"
	"time"
)

type Arguments struct {
	Name                 string
	Servers              []string
	AllowReconnect       bool
	MaxReconnect         int
	ReconnectWait        time.Duration
	Timeout              time.Duration
	DrainTimeout         time.Duration
	FlusherTimeout       time.Duration
	PingInterval         time.Duration
	MaxPingsOut          int
	SubChanLen           int
	User                 string
	Password             string
	Token                string
	RetryOnFailedConnect bool
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
	args         Arguments
	opts         nats.Options
	subscribeCh  chan subscribe
	schedulerCh  chan *worker
	ErrorHandler ErrorHandlerFunc
}

func New(args Arguments) *Conn {
	opts := nats.Options{
		Servers:              args.Servers,
		Name:                 args.Name,
		AllowReconnect:       args.AllowReconnect,
		MaxReconnect:         args.MaxReconnect,
		ReconnectWait:        args.ReconnectWait,
		Timeout:              args.Timeout,
		DrainTimeout:         args.DrainTimeout,
		FlusherTimeout:       args.FlusherTimeout,
		PingInterval:         args.PingInterval,
		MaxPingsOut:          args.MaxPingsOut,
		SubChanLen:           args.SubChanLen,
		User:                 args.User,
		Password:             args.Password,
		Token:                args.Token,
		RetryOnFailedConnect: args.RetryOnFailedConnect,
		Compression:          args.Compression,
		IgnoreAuthErrorAbort: args.IgnoreAuthErrorAbort,
		SkipHostLookup:       args.SkipHostLookup,
	}
	return &Conn{
		opts:        opts,
		subscribeCh: make(chan subscribe, 255),
		schedulerCh: make(chan *worker, 128),
	}
}

func (s *Conn) Start(ctx context.Context) error {
	nc, err := s.opts.Connect()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	var workers []*worker

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
			case s.schedulerCh <- w:
			case <-ctx.Done():
				break loop
			}

		case worker := <-s.schedulerCh:
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
