package connection

import (
	"fmt"
	"time"
)

type options struct {
	writeTimeoutCh   <-chan time.Time
	writeTimeoutFunc WriteTimeoutFunc

	readTimeoutCh   <-chan time.Time
	readTimeoutFunc ReadTimeoutFunc

	sendTimeoutCh <-chan time.Time

	dialTimeout time.Duration
}

type Option func(*options) error

// WithWriteTimeoutFunc sets a duration after which the connection is considered idle
// and the idle function is called.
// If duration is 0, the connection will never be considered idle.
func WithWriteTimeoutFunc(duration time.Duration, writeTimeoutFunc WriteTimeoutFunc) Option {
	return func(o *options) error {
		if duration == 0 && writeTimeoutFunc != nil {
			return fmt.Errorf("write timeout is set but write timeout duration is 0")
		}
		o.writeTimeoutCh = time.After(duration)
		o.writeTimeoutFunc = writeTimeoutFunc
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
		o.readTimeoutCh = time.After(duration)
		o.readTimeoutFunc = readTimeoutFunc
		return nil
	}
}

// WithSendTimeout sets a duration for which the connection will wait for a message to be sent.
func WithSendTimeout(duration time.Duration) Option {
	return func(o *options) error {
		o.sendTimeoutCh = time.After(duration)
		return nil
	}
}

// WithDialTimeout sets a duration for which the dialer will wait for a connection to be established.
func WithDialTimeout(duration time.Duration) Option {
	return func(o *options) error {
		o.dialTimeout = duration
		return nil
	}
}

func defaultOptions() options {
	return options{
		writeTimeoutCh:   nil,
		writeTimeoutFunc: nil,
		readTimeoutCh:    nil,
		readTimeoutFunc:  nil,
		sendTimeoutCh:    nil,
		dialTimeout:      5 * time.Second,
	}
}
