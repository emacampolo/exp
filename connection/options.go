package connection

import (
	"fmt"
	"time"
)

type options struct {
	writeTimeoutCh     <-chan time.Time
	writeTimeoutTicker *time.Ticker
	writeTimeoutFunc   WriteTimeoutFunc

	readTimeoutCh     <-chan time.Time
	readTimeoutTicker *time.Ticker
	readTimeoutFunc   ReadTimeoutFunc

	sendTimeoutCh <-chan time.Time

	sendTimeoutTicker *time.Ticker
	dialTimeout       time.Duration
}

type Option func(*options) error

// WithWriteTimeoutFunc sets a duration after which the connection is considered idle
// and the idle function is called.
// The duration must be greater than 0.
func WithWriteTimeoutFunc(duration time.Duration, writeTimeoutFunc WriteTimeoutFunc) Option {
	return func(o *options) error {
		if duration <= 0 {
			return fmt.Errorf("write timeout duration must be greater than 0")
		}

		o.writeTimeoutTicker = time.NewTicker(duration)
		o.writeTimeoutCh = o.writeTimeoutTicker.C
		o.writeTimeoutFunc = writeTimeoutFunc
		return nil
	}
}

// WithReadTimeoutFunc sets a duration for which the connection will wait for a message to be received.
// If duration is reached, the read timeout function is called.
// The duration must be greater than 0.
func WithReadTimeoutFunc(duration time.Duration, readTimeoutFunc ReadTimeoutFunc) Option {
	return func(o *options) error {
		if duration <= 0 {
			return fmt.Errorf("read timeout duration must be greater than 0")
		}

		o.readTimeoutTicker = time.NewTicker(duration)
		o.readTimeoutCh = o.readTimeoutTicker.C
		o.readTimeoutFunc = readTimeoutFunc
		return nil
	}
}

// WithSendTimeout sets a duration for which the connection will wait for a message to be sent.
// The duration must be greater than 0.
func WithSendTimeout(duration time.Duration) Option {
	return func(o *options) error {
		if duration <= 0 {
			return fmt.Errorf("send timeout duration must be greater than 0")
		}

		o.sendTimeoutTicker = time.NewTicker(duration)
		o.sendTimeoutCh = o.sendTimeoutTicker.C
		return nil
	}
}

// WithDialTimeout sets a duration for which the dialer will wait for a connection to be established.
func WithDialTimeout(duration time.Duration) Option {
	return func(o *options) error {
		if duration < 0 {
			return fmt.Errorf("dial timeout duration must be greater than 0")
		}
		o.dialTimeout = duration
		return nil
	}
}

func defaultOptions() options {
	return options{
		dialTimeout: 5 * time.Second,
	}
}
