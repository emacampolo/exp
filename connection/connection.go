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

// WriteTimeoutFunc is a function that is called when the connection is idle trying to write to the connection.
// A common use case is to send a heartbeat message to the server.
// The function is called in a separate goroutine.
type WriteTimeoutFunc func(connection *Connection)

// ReadTimeoutFunc is a function that is called when the connection is idle waiting for a message from the server
// for a period of time.
// The function is called in a separate goroutine.
type ReadTimeoutFunc func(connection *Connection)

// InboundMessageHandler is a function that is called when a message is received from the server
// and is not a response to a request sent using Send.
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

// Message represents a message sent to or received from the server.
// The Payload is any data that is marshaled and unmarshalled by the MarshalUnmarshaler.
type Message struct {
	// ID is the identification of the message that is used to match the response to the request.
	ID string
	// Payload is the message payload that is sent or received from the server.
	Payload any
}

// MarshalUnmarshaler is responsible for marshaling and unmarshalling the messages.
type MarshalUnmarshaler interface {
	Marshal(Message) ([]byte, error)
	Unmarshal([]byte) (Message, error)
}

// ErrorHandler is called in a goroutine with the errors that can't be returned to the caller.
type ErrorHandler func(err error)

// Connection represents a connection to a server.
type Connection struct {
	network string
	address string
	conn    io.ReadWriteCloser

	// outgoingChannel is used to send requests to the server.
	outgoingChannel chan request
	// incomingChannel is used to receive messages from the server.
	incomingChannel chan []byte
	// done is closed when the connection is closed and all messages are handled.
	done chan struct{}

	options options

	encodeDecoder      EncodeDecoder
	marshalUnmarshaler MarshalUnmarshaler
	errHandler         ErrorHandler
	handler            InboundMessageHandler

	mutex            sync.Mutex // guards pendingResponses map.
	pendingResponses map[string]response

	// messagesWg is used to wait for all the messages being sent and received to complete.
	messagesWg sync.WaitGroup

	closing atomic.Bool
}

// New creates a new connection to the server.
// All the argument but the options are required.
func New(network, address string, encodeDecoder EncodeDecoder, marshalUnmarshaler MarshalUnmarshaler, handler InboundMessageHandler, errHandler ErrorHandler, options ...Option) (*Connection, error) {
	opts := defaultOptions()
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			return nil, fmt.Errorf("applying option: %v", err)
		}
	}
	return &Connection{
		network:            network,
		address:            address,
		encodeDecoder:      encodeDecoder,
		marshalUnmarshaler: marshalUnmarshaler,
		handler:            handler,
		errHandler:         errHandler,

		options:          opts,
		outgoingChannel:  make(chan request),
		incomingChannel:  make(chan []byte),
		done:             make(chan struct{}),
		pendingResponses: make(map[string]response),
	}, nil
}

// NewFrom creates a new connection from an existing connection.
// All the argument but the options are required.
func NewFrom(conn io.ReadWriteCloser, encodeDecoder EncodeDecoder, marshalUnmarshaler MarshalUnmarshaler, handler InboundMessageHandler, errHandler ErrorHandler, options ...Option) (*Connection, error) {
	c, err := New("", "", encodeDecoder, marshalUnmarshaler, handler, errHandler, options...)
	if err != nil {
		return nil, fmt.Errorf("creating connection: %w", err)
	}
	c.conn = conn
	c.run()
	return c, nil
}

// Connect connects to the server.
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

// Address returns the address of the server that the connection is connected to.
// If the connection is not connected, it returns an empty string.
func (c *Connection) Address() string {
	return c.address
}

func (c *Connection) run() {
	go c.writeLoop()
	go c.readLoop()
	go c.handleLoop()
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
	c.messagesWg.Wait()
	defer close(c.done)

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

// response represents a response from the server.
type response struct {
	replyCh chan Message
	errCh   chan error
}

// Send sends message and waits for the response.
// If the response is not received within the timeout, it returns ErrSendTimeout.
// If the connection is closed, it returns ErrConnectionClosed.
func (c *Connection) Send(message Message) (Message, error) {
	c.messagesWg.Add(1)
	defer c.messagesWg.Done()

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

	c.outgoingChannel <- req

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
	c.messagesWg.Add(1)
	defer c.messagesWg.Done()

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

	c.outgoingChannel <- req
	err = <-req.errCh
	return err
}

// writeLoop read requests from the outgoingChannel and writes them to the connection.
func (c *Connection) writeLoop() {
	var err error

	for err == nil {
		select {
		case req := <-c.outgoingChannel:
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
		case <-c.options.writeTimeoutCh:
			if c.options.writeTimeoutFunc != nil {
				go c.options.writeTimeoutFunc(c)
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

// handlerConnectionError is called when we get an error when reading or writing to the connection.
// It closes the connection.
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

	// Return the error to the callers of Send and Reply.
	go func() {
		for {
			select {
			case req := <-c.outgoingChannel:
				req.errCh <- ErrConnectionClosed
			case <-done:
				return
			}
		}
	}()

	go func() {
		c.messagesWg.Wait()
		done <- struct{}{}
	}()

	c.close()
}

// readLoop reads data from the socket and runs a goroutine to handle the message.
// It reads data from the socket until the connection is closed.
// We should not return from Close until all the messages are handled.
func (c *Connection) readLoop() {
	var err error
	var messageBytes []byte

	for {
		messageBytes, err = c.encodeDecoder.Decode(c.conn)
		if err != nil {
			c.handleError(err)
			break
		}

		c.incomingChannel <- messageBytes
	}

	c.handleConnectionError(err)
}

// handleLoop handles the incoming messages that are read from the socket by the readLoop.
// The messages may correspond to the responses to the requests sent by the Send method
// or, they may be messages sent by the server to the client without a request.
func (c *Connection) handleLoop() {
	for {
		select {
		case rawBytes := <-c.incomingChannel:
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
		c.messagesWg.Add(1)
		c.handler(c, message)
		c.messagesWg.Done()
	}
}
