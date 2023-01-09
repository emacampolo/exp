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
//   - To receive messages that are sent by the remote host, you need to implement the InboundMessageHandler function.
//   - To send messages to the remote host, you can either use the Request function or the Send function.
//     The difference between the two is that the Request resembles to the Request-Response pattern in that it expects
//     a response from the remote host.
//     The Send function doesn't expect a response from the remote host.
//     In this regard, it can be related to the Fire-Forget pattern.
//
// It is important to close the underlying connection using the Close method of this interface
// to ensure that all the resources are released properly and all the requests are completed.
// It is safe to call methods of the connection from multiple goroutines.
type Connection interface {
	// Request sends message and waits for the response as you would expect from the Request-Response pattern.
	// It returns a ErrRequestTimeout error if the response is not received within the timeout.
	// If the Connection is closed, it returns ErrConnectionClosed.
	Request(message Message) (response Message, err error)

	// Send sends message and does not wait for the response as you would expect from the Fire-Forget pattern.
	// If the Connection is closed, it returns ErrConnectionClosed.
	Send(message Message) error

	// Address returns the address of the host that the Connection is connected to.
	Address() string

	// Close closes the Connection. It waits for all requests to complete.
	// If the Connection is already closed, it returns nil.
	// The underlying connection is closed as well.
	Close() error
}

// InboundMessageHandler is a function that is called when a message is received from the remote host.
// The function is called in a separate goroutine.
// The function should return a response to be sent back to the remote host.
type InboundMessageHandler func(connection Connection, message Message)

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
	Marshal(Message) ([]byte, error)
	// Unmarshal unmarshalls the message from a byte array.
	Unmarshal([]byte) (Message, error)
}

// Message represents a message that is sent over the connection in either direction.
// The Payload is any data that is marshaled and unmarshalled by the MarshalUnmarshaler.
type Message struct {
	// ID is the identification of the message that is used to match the response to the request.
	ID string
	// Payload is the message payload.
	Payload any
}

// ErrorHandler is called in a goroutine with the errors that can't be returned to the caller.
type ErrorHandler func(err error)

// New returns a new connection that is created from the given net.Conn.
// All the arguments are required.
func New(conn net.Conn, encodeDecoder EncodeDecoder, marshalUnmarshaler MarshalUnmarshaler, handler InboundMessageHandler, options *Options) (Connection, error) {
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

	if options == nil {
		return nil, errors.New("options is nil")
	}

	c := newConnection(conn, encodeDecoder, marshalUnmarshaler, handler, options)
	c.run()
	return c, nil
}

type connection struct {
	conn               net.Conn
	encodeDecoder      EncodeDecoder
	marshalUnmarshaler MarshalUnmarshaler
	handler            InboundMessageHandler
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

func newConnection(conn net.Conn, encodeDecoder EncodeDecoder, marshalUnmarshaler MarshalUnmarshaler, handler InboundMessageHandler, options *Options) *connection {
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

	c.pendingResponsesMutex.Lock()
	response, found := c.pendingResponses[message.ID]
	c.pendingResponsesMutex.Unlock()

	if found {
		response.replyCh <- message
	} else {
		c.closingMutex.Lock()
		if c.closing {
			c.closingMutex.Unlock()
			return
		}

		c.messagesWg.Add(1)
		c.closingMutex.Unlock()

		c.handler(c, message)
		c.messagesWg.Done()
	}
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
	replyCh   chan Message
	errCh     chan error
}

// response represents a response from the remote host.
type response struct {
	replyCh chan Message
	errCh   chan error
}

// Request implements the Connection interface.
func (c *connection) Request(message Message) (Message, error) {
	c.closingMutex.Lock()
	if c.closing {
		c.closingMutex.Unlock()
		return Message{}, ErrConnectionClosed
	}

	c.messagesWg.Add(1)
	c.closingMutex.Unlock()

	defer c.messagesWg.Done()

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

	var resp Message

	select {
	case resp = <-req.replyCh:
	case err = <-req.errCh:
	case <-c.options.requestTimeoutCh:
		err = ErrRequestTimeout
		// If the request times out it means that the caller will no longer wait for the response
		// on the replyCh.
		// If the response is received before removing the pending response from the map,
		// the response will be lost.
		// To avoid that, we forward the response to the InboundMessageHandler.
		go func() {
			select {
			case resp = <-req.replyCh:
				go c.handler(c, resp)
			case <-c.done:
			}
		}()
	}

	c.pendingResponsesMutex.Lock()
	delete(c.pendingResponses, req.requestID)
	c.pendingResponsesMutex.Unlock()

	return resp, err
}

// Send implements the Connection interface.
func (c *connection) Send(message Message) error {
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
