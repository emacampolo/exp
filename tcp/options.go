package tcp

import (
	"fmt"
	"time"
)

type options struct {
	idleTimeout     time.Duration
	idleFunc        IdleFunc
	readTimeout     time.Duration
	readTimeoutFunc ReadTimeoutFunc
	dialTimeout     time.Duration
	sendTimeout     time.Duration
}

type Option func(*options) error

// WithIdleFunc sets a duration after which the connection is considered idle
// and the idle function is called.
// If duration is 0, the connection will never be considered idle.
func WithIdleFunc(duration time.Duration, idleFunc IdleFunc) Option {
	return func(o *options) error {
		if duration == 0 && idleFunc != nil {
			return fmt.Errorf("idle function is set but idle duration is 0")
		}
		o.idleTimeout = duration
		o.idleFunc = idleFunc
		return nil
	}
}

// WithReadTimeoutFunc sets a duration for which the connection will wait for a message to be received.
// If duration is reached, the read timeout function is called.
// If duration is 0, the connection will never time out.
func WithReadTimeoutFunc(duration time.Duration, readTimeoutFunc ReadTimeoutFunc) Option {
	return func(o *options) error {
		if duration == 0 && readTimeoutFunc != nil {
			return fmt.Errorf("read timeout function is set but read timeout duration is 0")
		}
		o.readTimeout = duration
		o.readTimeoutFunc = readTimeoutFunc
		return nil
	}
}

func defaultOptions() options {
	return options{
		idleTimeout: 0,
		idleFunc:    nil,
		readTimeout: 0,
		dialTimeout: 5 * time.Second,
		sendTimeout: 30 * time.Second,
	}
}
