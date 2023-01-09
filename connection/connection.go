package connection

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

// Connection represents a bidirectional communication channel between hosts.
// It is worth mentioning that we don't use the term "client" and "server". Instead, we use the terms "host" and "remote host".
// It requires a net.Conn which can be created using net.Dial or net.Listen.
// Its main purpose is to send and receive messages.
//   - To receive messages that are sent by the remote host, you need to implement the Handler and NetworkHandler interfaces.
//   - To send messages to the remote host, you can either use the Request function or the Send function.
//     The difference between the two is that the Request resembles to the Request-Response pattern in that it expects
//     a response from the remote host.
//     The Send function doesn't expect a response from the remote host.
//     In this regard, it can be related to the Fire-Forget pattern.
//     Both ensures that the message is written to the socket before returning the control to the caller.
//
// It is important to close the underlying connection using the Close method of this interface
// to ensure that all the resources are released properly and all the requests are completed.
// It is safe to call methods of the connection from multiple goroutines.
type Connection interface {
	// Request sends message and waits for the response as you would expect from the Request-Response pattern.
	// It returns a ErrRequestTimeout error if the response is not received within the timeout.
	// If the Connection is closed, it returns ErrConnectionClosed.
	Request(message OutboundMessage) (response InboundMessage, err error)

	// Send sends message and does not wait for the response as you would expect from the Fire-Forget pattern.
	// If the Connection is closed, it returns ErrConnectionClosed.
	Send(message OutboundMessage) error

	// Address returns the address of the host that the Connection is connected to.
	Address() string

	// Close closes the Connection. It waits for all requests to complete.
	// If the Connection is already closed, it returns nil.
	// The underlying connection is closed as well.
	Close() error
}

// Handler is the interface that uses the Connection to handle incoming messages from the remote host
// that are not responses to requests nor network messages.
type Handler interface {
	Handle(connection Connection, message InboundMessage)
}

// NetworkHandler is the interface that uses the Connection to handle incoming messages from the remote host
// that are network messages.
type NetworkHandler interface {
	Handle(connection Connection, message InboundMessage)
}

// WriteTimeoutFunc is a function that is called when the Connection is idle trying to write to the underlying connection.
// A common use case is to send a heartbeat message to the remote destination.
// The function is called in a separate goroutine.
type WriteTimeoutFunc func(connection Connection)

// ReadTimeoutFunc is a function that is called when the connection is idle waiting for a message from the remote host
// for a period of time.
// The function is called in a separate goroutine.
type ReadTimeoutFunc func(connection Connection)

var (
	// ErrConnectionClosed is returned when the connection is closed by any of the parties.
	ErrConnectionClosed = errors.New("connection closed")
	// ErrRequestTimeout is returned when the request is not completed within the specified timeout.
	ErrRequestTimeout = errors.New("request timeout")
	// ErrUnexpectedResponse is returned when the response is not expected.
	ErrUnexpectedResponse = errors.New("unexpected response")
)

// EncodeDecoder is responsible for encoding and decoding the byte representation of a Message.
// This interface it is used in conjunction with the MarshalUnmarshaler interface.
// Any returned error is considered fatal and therefore the connection is closed.
type EncodeDecoder interface {
	// Encode encodes the message into a writer. It is safe call Write more than once.
	Encode(writer io.Writer, message []byte) error
	// Decode decodes the message from a reader. It is responsible for reading an entire message.
	// It shouldn't read more that necessary.
	Decode(reader io.Reader) (message []byte, err error)
}

// MarshalUnmarshaler is responsible for marshaling and unmarshalling the messages.
// Any returned error is considered fatal and therefore the connection is closed.
type MarshalUnmarshaler interface {
	// Marshal marshals the message into a byte array.
	Marshal(OutboundMessage) ([]byte, error)
	// Unmarshal unmarshalls the message from a byte array.
	Unmarshal([]byte) (InboundMessage, error)
}

// OutboundMessage represents a message that is sent to the remote host.
type OutboundMessage struct {
	// ID is the identification of the message that is used to match the response to the request.
	ID string

	// Payload is the message payload.
	Payload any
}

// InboundMessage represents a message that is received from the remote host.
type InboundMessage struct {
	// ID is the identification of the message that is used to match the response to the request.
	ID string

	// Payload is the message payload.
	Payload any

	// IsResponse is true if the message is a response to a request.
	IsResponse bool

	// IsNetwork is true if the message is network message such as a heartbeat or a ping.
	IsNetwork bool
}

// ErrorHandler is called in a goroutine with the errors that can't be returned to the caller.
type ErrorHandler func(err error)

// New returns a new connection that is created from the given net.Conn.
// All the arguments are required.
func New(conn net.Conn, encodeDecoder EncodeDecoder, marshalUnmarshaler MarshalUnmarshaler,
	handler Handler, networkHandler NetworkHandler, options *Options) (Connection, error) {
	if conn == nil {
		return nil, errors.New("connection is nil")
	}

	if encodeDecoder == nil {
		return nil, errors.New("encode decoder is nil")
	}

	if marshalUnmarshaler == nil {
		return nil, errors.New("marshal unmarshaler is nil")
	}

	if handler == nil {
		return nil, errors.New("handler is nil")
	}

	if networkHandler == nil {
		return nil, errors.New("network handler is nil")
	}

	if options == nil {
		return nil, errors.New("options is nil")
	}

	c := newConnection(conn, encodeDecoder, marshalUnmarshaler, handler, networkHandler, options)
	c.run()
	return c, nil
}

type connection struct {
	conn               net.Conn
	encodeDecoder      EncodeDecoder
	marshalUnmarshaler MarshalUnmarshaler
	handler            Handler
	networkHandler     NetworkHandler
	options            *Options

	// outgoingChannel is used to send requests to the remote host.
	outgoingChannel chan request
	// incomingChannel is used to receive messages from the remote host.
	incomingChannel chan []byte
	// done is closed when the connection is closed and all messages are handled.
	done chan struct{}

	pendingResponsesMutex sync.Mutex // guards pendingResponses map.
	pendingResponses      map[string]response

	closingMutex sync.Mutex // guards messagesWg and closing.
	messagesWg   sync.WaitGroup
	closing      bool

	stopTickersFunc func()
}

func newConnection(conn net.Conn, encodeDecoder EncodeDecoder, marshalUnmarshaler MarshalUnmarshaler,
	handler Handler, networkHandler NetworkHandler, options *Options) *connection {
	// When the connection is closed, stop all the tickers to avoid leakage.
	stopTickersFunc := func() {
		if options.writeTimeoutTicker != nil {
			options.writeTimeoutTicker.Stop()
		}

		if options.readTimeoutTicker != nil {
			options.readTimeoutTicker.Stop()
		}

		if options.requestTimeoutTicker != nil {
			options.requestTimeoutTicker.Stop()
		}
	}

	return &connection{
		// Arguments passed to the constructor.
		conn:               conn,
		encodeDecoder:      encodeDecoder,
		marshalUnmarshaler: marshalUnmarshaler,
		handler:            handler,
		networkHandler:     networkHandler,
		options:            options,

		outgoingChannel:  make(chan request),
		incomingChannel:  make(chan []byte),
		done:             make(chan struct{}),
		pendingResponses: make(map[string]response),
		stopTickersFunc:  stopTickersFunc,
	}
}

// Address implements the Connection interface.
func (c *connection) Address() string {
	return c.conn.RemoteAddr().String()
}

func (c *connection) run() {
	go c.writeLoop()
	go c.readLoop()
	go c.handleLoop()
}

// writeLoop read requests from the outgoingChannel and writes them to the connection.
func (c *connection) writeLoop() {
	var err error

	for err == nil {
		select {
		case req := <-c.outgoingChannel:
			if req.replyCh != nil {
				c.pendingResponsesMutex.Lock()
				c.pendingResponses[req.requestID] = response{
					replyCh: req.replyCh,
					errCh:   req.errCh,
				}
				c.pendingResponsesMutex.Unlock()
			}

			_, err = c.conn.Write(req.message)
			if err != nil {
				c.handleError(err)
				break
			}

			// For messages using Send we just
			// return nil to errCh as caller is waiting for error.
			// Messages sent by Request waits for responses to be received to their replyCh channel.
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

// readLoop reads data from the socket and runs a goroutine to handle the message.
// It reads data from the socket until the connection is closed.
// We should not return from Close until all the messages are handled.
func (c *connection) readLoop() {
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
// The messages may correspond to the responses to the requests sent by the Request method
// or, they may be messages sent by the remote host to the local host without a previous request.
func (c *connection) handleLoop() {
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

func (c *connection) handleResponse(rawBytes []byte) {
	message, err := c.marshalUnmarshaler.Unmarshal(rawBytes)
	if err != nil {
		c.handleError(err)
		return
	}

	// If the message is a response to a request, we should send its channel.
	// If it is not found in the pendingResponses map, it means that the request timed out.
	// In that case, we should just ignore the response but call the handlerError function with a sentinel error.
	if message.IsResponse {
		c.pendingResponsesMutex.Lock()
		response, found := c.pendingResponses[message.ID]
		c.pendingResponsesMutex.Unlock()
		if found {
			response.replyCh <- message
		} else {
			c.handleError(ErrUnexpectedResponse)
		}
		return
	}

	if message.IsNetwork {
		c.networkHandler.Handle(c, message)
		return
	}

	// If it is a message sent by the remote host to the local host without a previous request,
	// we need to increment the wait group to avoid returning from Close before the message is handled.
	c.closingMutex.Lock()
	if c.closing {
		c.closingMutex.Unlock()
		return
	}
	c.messagesWg.Add(1)
	c.closingMutex.Unlock()
	c.handler.Handle(c, message)
	c.messagesWg.Done()
}

func (c *connection) handleError(err error) {
	if c.options.errorHandler == nil {
		return
	}

	// When the connection is closed, it is expected that either the read or write loop will return an error.
	// In that case, we don't need to call the error handler.
	c.closingMutex.Lock()
	if c.closing {
		c.closingMutex.Unlock()
		return
	}
	c.closingMutex.Unlock()

	go c.options.errorHandler(err)
}

// handlerConnectionError is called when we get an error when reading or writing to the underlying connection.
func (c *connection) handleConnectionError(err error) {
	c.closingMutex.Lock()
	if err == nil || c.closing {
		c.closingMutex.Unlock()
		return
	}

	c.closing = true
	c.closingMutex.Unlock()

	done := make(chan struct{})

	c.pendingResponsesMutex.Lock()
	for _, resp := range c.pendingResponses {
		resp.errCh <- ErrConnectionClosed
	}
	c.pendingResponsesMutex.Unlock()

	// Return the error to the callers of Request and Send.
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

func (c *connection) close() error {
	// Last thing to do is mark this connection as done.
	defer close(c.done)

	defer c.stopTickersFunc()

	c.messagesWg.Wait()
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("closing connection: %w", err)
	}

	return nil
}

// Close implements the Connection interface.
func (c *connection) Close() error {
	c.closingMutex.Lock()

	if c.closing {
		c.closingMutex.Unlock()
		return nil
	}
	c.closing = true
	c.closingMutex.Unlock()

	return c.close()
}

// Done returns a channel that is closed when the connection is closed and all messages are handled.
func (c *connection) Done() <-chan struct{} {
	return c.done
}

// request represents a request to the remote host.
type request struct {
	message   []byte
	requestID string
	replyCh   chan InboundMessage
	errCh     chan error
}

// response represents a response from the remote host.
type response struct {
	replyCh chan InboundMessage
	errCh   chan error
}

// Request implements the Connection interface.
func (c *connection) Request(message OutboundMessage) (InboundMessage, error) {
	c.closingMutex.Lock()
	if c.closing {
		c.closingMutex.Unlock()
		return InboundMessage{}, ErrConnectionClosed
	}

	c.messagesWg.Add(1)
	c.closingMutex.Unlock()

	defer c.messagesWg.Done()

	if message.ID == "" {
		return InboundMessage{}, errors.New("message ID is empty")
	}

	messageBytes, err := c.marshalUnmarshaler.Marshal(message)
	if err != nil {
		return InboundMessage{}, fmt.Errorf("marshaling message: %w", err)
	}

	var buff bytes.Buffer
	if err := c.encodeDecoder.Encode(&buff, messageBytes); err != nil {
		return InboundMessage{}, err
	}

	req := request{
		message:   buff.Bytes(),
		requestID: message.ID,
		replyCh:   make(chan InboundMessage),
		errCh:     make(chan error),
	}

	c.outgoingChannel <- req

	var resp InboundMessage

	select {
	case resp = <-req.replyCh:
	case err = <-req.errCh:
	case <-c.options.requestTimeoutCh:
		err = ErrRequestTimeout
	}

	c.pendingResponsesMutex.Lock()
	delete(c.pendingResponses, req.requestID)
	c.pendingResponsesMutex.Unlock()

	return resp, err
}

// Send implements the Connection interface.
func (c *connection) Send(message OutboundMessage) error {
	c.closingMutex.Lock()
	if c.closing {
		c.closingMutex.Unlock()
		return ErrConnectionClosed
	}

	c.messagesWg.Add(1)
	c.closingMutex.Unlock()

	defer c.messagesWg.Done()

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
