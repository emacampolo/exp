package connection

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// IdleFunc is a function that is called when the connection is idle for a period of time.
// A common use case is to send a heartbeat message to the server.
// The function is called in a separate goroutine.
type IdleFunc func(connection *Connection)

// ReadTimeoutFunc is a function that is called when the connection is idle waiting for a
// message from the server for a period of time.
// The function is called in a separate goroutine.
type ReadTimeoutFunc func(connection *Connection)

// InboundMessageHandler is a function that is called when a message is received from the server and,
// it is not a response to a request.
// The function is called in a separate goroutine.
// The function should return a response to be sent back to the server.
type InboundMessageHandler func(connection *Connection, message Message)

var (
	// ErrConnectionClosed is returned when the connection is closed.
	ErrConnectionClosed = errors.New("connection closed")
	// ErrSendTimeout is returned when the send timeout is reached.
	ErrSendTimeout = errors.New("message send timeout")
)

// EncodeDecoder is responsible for encoding and decoding the messages.
type EncodeDecoder interface {
	Encode(writer io.Writer, message []byte) error
	Decode(reader io.Reader) ([]byte, error)
}

// Message is a message that is sent or received from the server.
type Message struct {
	// ID is the identification of the message that is used to match the response to the request.
	ID string
	// Payload is the message payload that is sent or received from the server.
	Payload any
}

// MarshalUnmarshaler is responsible for marshaling and un-marshaling the messages.
type MarshalUnmarshaler interface {
	Marshal(Message) ([]byte, error)
	Unmarshal([]byte) (Message, error)
}

// ErrorHandler is called in a goroutine with the errors that can't be returned to the caller
type ErrorHandler func(err error)

// Connection represents a connection to a server.
type Connection struct {
	network        string
	address        string
	conn           io.ReadWriteCloser
	requestsCh     chan request
	readResponseCh chan []byte
	done           chan struct{}

	options options

	encodeDecoder      EncodeDecoder
	marshalUnmarshaler MarshalUnmarshaler
	errHandler         ErrorHandler
	handler            InboundMessageHandler

	mutex            sync.Mutex // guards pendingResponses map.
	pendingResponses map[string]response

	wg sync.WaitGroup

	closing atomic.Bool
}

func New(network, address string, encDec EncodeDecoder, marshalUnmarshaler MarshalUnmarshaler, handler InboundMessageHandler, errHandler ErrorHandler, options ...Option) (*Connection, error) {
	opts := defaultOptions()
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			return nil, fmt.Errorf("applying option: %v", err)
		}
	}
	return &Connection{
		network:            network,
		address:            address,
		encodeDecoder:      encDec,
		marshalUnmarshaler: marshalUnmarshaler,
		errHandler:         errHandler,
		handler:            handler,
		options:            opts,
		requestsCh:         make(chan request),
		readResponseCh:     make(chan []byte),
		done:               make(chan struct{}),
		pendingResponses:   make(map[string]response),
	}, nil
}

func NewFrom(conn io.ReadWriteCloser, encDec EncodeDecoder, marshalUnmarshaler MarshalUnmarshaler, handler InboundMessageHandler, errHandler ErrorHandler, options ...Option) (*Connection, error) {
	c, err := New("", "", encDec, marshalUnmarshaler, handler, errHandler, options...)
	if err != nil {
		return nil, fmt.Errorf("creating connection: %w", err)
	}
	c.conn = conn
	c.run()
	return c, nil
}

func (c *Connection) Connect() error {
	if c.conn != nil {
		c.run()
		return nil
	}

	conn, err := net.DialTimeout(c.network, c.address, c.options.dialTimeout)
	if err != nil {
		return fmt.Errorf("dialing: %w", err)
	}

	c.conn = conn
	c.address = conn.RemoteAddr().String()
	c.run()

	return nil
}

func (c *Connection) Address() string {
	return c.address
}

func (c *Connection) run() {
	go c.writeLoop()
	go c.readLoop()
	go c.readResponseLoop()
}

// Close closes the connection. It waits for all requests to complete before closing the connection.
// It is safe to call Close multiple times.
func (c *Connection) Close() error {
	if c.closing.Swap(true) {
		return nil
	}

	return c.close()
}

func (c *Connection) close() error {
	// wait for all requests to complete before closing the connection
	c.wg.Wait()

	close(c.done)

	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			return fmt.Errorf("closing connection: %w", err)
		}
	}

	return nil
}

// Done returns a channel that is closed when the connection is closed.
func (c *Connection) Done() <-chan struct{} {
	return c.done
}

// request represents a request to the server.
type request struct {
	message   []byte
	requestID string
	replyCh   chan Message
	errCh     chan error
}

type response struct {
	replyCh chan Message
	errCh   chan error
}

// Send sends message and waits for the response.
// If the response is not received within the timeout, it returns ErrSendTimeout.
// If the connection is closed, it returns ErrConnectionClosed.
func (c *Connection) Send(message Message) (Message, error) {
	c.wg.Add(1)
	defer c.wg.Done()

	if c.closing.Load() {
		return Message{}, ErrConnectionClosed
	}

	if message.ID == "" {
		return Message{}, errors.New("message ID is empty")
	}

	messageBytes, err := c.marshalUnmarshaler.Marshal(message)
	if err != nil {
		return Message{}, fmt.Errorf("marshaling message: %w", err)
	}

	var buff bytes.Buffer
	if err := c.encodeDecoder.Encode(&buff, messageBytes); err != nil {
		return Message{}, err
	}

	req := request{
		message:   buff.Bytes(),
		requestID: message.ID,
		replyCh:   make(chan Message),
		errCh:     make(chan error),
	}

	c.requestsCh <- req

	var response Message

	select {
	case response = <-req.replyCh:
	case err = <-req.errCh:
		return Message{}, err
	case <-c.options.sendTimeoutCh:
		return Message{}, ErrSendTimeout
	}

	c.mutex.Lock()
	delete(c.pendingResponses, req.requestID)
	c.mutex.Unlock()

	return response, nil
}

// Reply sends message and does not wait for the response.
// If the connection is closed, it returns ErrConnectionClosed.
func (c *Connection) Reply(message Message) error {
	c.wg.Add(1)
	defer c.wg.Done()

	if c.closing.Load() {
		return ErrConnectionClosed
	}

	messageBytes, err := c.marshalUnmarshaler.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}

	var buff bytes.Buffer
	if err := c.encodeDecoder.Encode(&buff, messageBytes); err != nil {
		return err
	}

	req := request{
		message: buff.Bytes(),
		errCh:   make(chan error),
	}

	c.requestsCh <- req
	err = <-req.errCh
	return err
}

// writeLoop read requests from the requestsCh and writes them to the connection.
func (c *Connection) writeLoop() {
	var err error

	for err == nil {
		select {
		case req := <-c.requestsCh:
			if req.replyCh != nil {
				c.mutex.Lock()
				c.pendingResponses[req.requestID] = response{
					replyCh: req.replyCh,
					errCh:   req.errCh,
				}
				c.mutex.Unlock()
			}

			_, err = c.conn.Write(req.message)
			if err != nil {
				c.handleError(err)
				break
			}

			// For replies (requests without replyCh) we just
			// return nil to errCh as caller is waiting for error
			// or send timeout. Regular requests waits for responses
			// to be received to their replyCh channel.
			if req.replyCh == nil {
				req.errCh <- nil
			}
		case <-c.options.idleTimeoutCh:
			if c.options.idleFunc != nil {
				go c.options.idleFunc(c)
			}
		case <-c.done:
			return
		}
	}

	c.handleConnectionError(err)
}

func (c *Connection) handleError(err error) {
	if c.errHandler == nil {
		return
	}

	// When the connection is closed, it is expected that either the read or write loop will return an error.
	// In that case, we don't need to call the error handler.
	if c.closing.Load() {
		return
	}

	go c.errHandler(err)
}

// handlerConnectionError handles the connection error by closing the connection.
func (c *Connection) handleConnectionError(err error) {
	if err == nil || c.closing.Swap(true) {
		return
	}

	done := make(chan struct{})

	c.mutex.Lock()
	for _, resp := range c.pendingResponses {
		resp.errCh <- ErrConnectionClosed
	}
	c.mutex.Unlock()

	// Return the error to the callers of Send.
	go func() {
		for {
			select {
			case req := <-c.requestsCh:
				req.errCh <- ErrConnectionClosed
			case <-done:
				return
			}
		}
	}()

	go func() {
		c.wg.Wait()
		done <- struct{}{}
	}()

	c.close()
}

// readLoop reads data from the socket and runs a goroutine to handle the message.
func (c *Connection) readLoop() {
	var err error
	var messageBytes []byte

	for {
		messageBytes, err = c.encodeDecoder.Decode(c.conn)
		if err != nil {
			c.handleError(err)
			break
		}

		c.readResponseCh <- messageBytes
	}

	c.handleConnectionError(err)
}

func (c *Connection) readResponseLoop() {
	for {
		select {
		case rawBytes := <-c.readResponseCh:
			go c.handleResponse(rawBytes)
		case <-c.options.readTimeoutCh:
			if c.options.readTimeoutFunc != nil {
				go c.options.readTimeoutFunc(c)
			}
		case <-c.done:
			return
		}
	}
}

func (c *Connection) handleResponse(rawBytes []byte) {
	message, err := c.marshalUnmarshaler.Unmarshal(rawBytes)
	if err != nil {
		c.handleError(err)
		return
	}

	c.mutex.Lock()
	response, found := c.pendingResponses[message.ID]
	c.mutex.Unlock()

	if found {
		response.replyCh <- message
	} else {
		c.handler(c, message)
	}
}
