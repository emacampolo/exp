package connection

import (
	"fmt"
	"time"
)

// Options contains options for a Connection.
type Options struct {
	writeTimeoutCh     <-chan time.Time
	writeTimeoutTicker *time.Ticker
	writeTimeoutFunc   WriteTimeoutFunc

	readTimeoutCh     <-chan time.Time
	readTimeoutTicker *time.Ticker
	readTimeoutFunc   ReadTimeoutFunc

	requestTimeoutCh     <-chan time.Time
	requestTimeoutTicker *time.Ticker

	errorHandler ErrorHandler
}

// NewOptions creates a new Options instance with sensible defaults.
func NewOptions() *Options {
	return &Options{}
}

// SetWriteTimeoutFunc sets a duration after which the connection is considered idle
// and the idle function is called.
// The duration must be greater than 0.
func (o *Options) SetWriteTimeoutFunc(duration time.Duration, writeTimeoutFunc WriteTimeoutFunc) error {
	if duration <= 0 {
		return fmt.Errorf("write timeout duration must be greater than 0")
	}

	o.writeTimeoutTicker = time.NewTicker(duration)
	o.writeTimeoutCh = o.writeTimeoutTicker.C
	o.writeTimeoutFunc = writeTimeoutFunc
	return nil
}

// SetReadTimeoutFunc sets a duration for which the connection will wait for a message to be received.
// If duration is reached, the read timeout function is called.
// The duration must be greater than 0.
func (o *Options) SetReadTimeoutFunc(duration time.Duration, readTimeoutFunc ReadTimeoutFunc) error {
	if duration <= 0 {
		return fmt.Errorf("read timeout duration must be greater than 0")
	}

	o.readTimeoutTicker = time.NewTicker(duration)
	o.readTimeoutCh = o.readTimeoutTicker.C
	o.readTimeoutFunc = readTimeoutFunc
	return nil
}

// SetRequestTimeout sets a duration for which the connection will wait for a message to be sent.
// The duration must be greater than 0.
func (o *Options) SetRequestTimeout(duration time.Duration) error {
	if duration <= 0 {
		return fmt.Errorf("request timeout duration must be greater than 0")
	}

	o.requestTimeoutTicker = time.NewTicker(duration)
	o.requestTimeoutCh = o.requestTimeoutTicker.C
	return nil
}

// SetErrorHandler sets a function to be called when an error occurs while trying
// to unmarshal a message or when an error occurs while trying to write a message to the connection.
// The error handler is called with the error that occurred.
func (o *Options) SetErrorHandler(errorHandler ErrorHandler) {
	o.errorHandler = errorHandler
}
