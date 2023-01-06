package connection

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

// Connection represents a connection to a remote host. It requires a net.Conn which can be created using
// net.Dial or net.Listen.
// In the scope of this package, a Connection is a bidirectional communication channel between 2 hosts.
// It is worth mentioning that we don't use the term "client" and "server". Instead, we use the terms "host" and "remote host".
// It is important to close the underlying connection using the Close method of this interface
// to ensure that all the resources are released properly and all the requests are completed.
// It is safe to call methods of the connection from multiple goroutines.
type Connection interface {
	// Send sends message and waits for the response.
	// If the response is not received within the timeout, it returns ErrSendTimeout.
	// If the Connection is closed, it returns ErrConnectionClosed.
	Send(message Message) (response Message, err error)

	// Reply sends message and does not wait for the response.
	// If the Connection is closed, it returns ErrConnectionClosed.
	Reply(message Message) error

	// Address returns the address of the host that the Connection is connected to.
	Address() string

	// Close closes the Connection. It waits for all requests to complete.
	// It is safe to call Close multiple times.
	Close() error
}

// InboundMessageHandler is a function that is called when a message is received from the remote host
// and is not a response to a request sent using Connection.Send.
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
	// ErrSendTimeout is returned when the send timeout is reached.
	ErrSendTimeout = errors.New("message send timeout")
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

		if options.sendTimeoutTicker != nil {
			options.sendTimeoutTicker.Stop()
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

func (c *connection) close() error {
	defer c.stopTickersFunc()
	c.messagesWg.Wait()

	defer close(c.done)

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("closing connection: %w", err)
	}

	return nil
}

// Done returns a channel that is closed when the connection is closed.
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

// Send implements the Connection interface.
func (c *connection) Send(message Message) (Message, error) {
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

	var response Message

	select {
	case response = <-req.replyCh:
	case err = <-req.errCh:
		return Message{}, err
	case <-c.options.sendTimeoutCh:
		return Message{}, ErrSendTimeout
	}

	c.pendingResponsesMutex.Lock()
	delete(c.pendingResponses, req.requestID)
	c.pendingResponsesMutex.Unlock()

	return response, nil
}

// Reply implements the Connection interface.
func (c *connection) Reply(message Message) error {
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
// The messages may correspond to the responses to the requests sent by the Send method
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
