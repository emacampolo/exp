# Package connection

This package is heavily inspired by https://github.com/moov-io/iso8583-connection. 
The key differences are:

- The messaging protocol is not tied to ISO8583 or the usage of [moov-io/iso8583](https://github.com/moov-io/iso8583).
- It Provides a higher level abstraction for sending and receiving messages over a connection.
- It ensures that when Close is called, the function will only return once all messages sent using `Send` or `Reply` 
have been processed, or any `Message` forwarded to `InboundMessageHandler` has finished being handled.

## Usage

```go
package main

import (
	"log"
	"time"

	"github.com/emacampolo/exp/connection"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	conn, err := connection.New("tcp", "127.0.0.1:9999", encodeDecoder{}, marshalUnmarshal{}, handler,
		connection.WithSendTimeout(5*time.Second))
	if err != nil {
		return err
	}

	// Start the connection by connecting to the server.
	if err := conn.Connect(); err != nil {
		return err
	}

	defer conn.Close()

	// Send a message to the server and wait for a response.
	// If the server does not respond within the send timeout duration, it returns an ErrSendTimeout error.
	result, err := conn.Send(connection.Message{ID: "1", Payload: "ping"})
	if err != nil {
		return err
	}

	log.Printf("result: %v", result)
	return nil
}
```

## Options

The following options are available when creating a new connection:

- `WithWriteTimeoutFunc` - A function that is called when there is no message sent to the server within a given
  duration. 
- `WithReadTimeoutFunc` - A function that is called when there is no message received from the server within a given
  duration.
- `WithSendTimeout` - The duration to wait for a response from the server when sending a `Message`.
- `WithDialTimeout` - The duration to wait for a connection to be established.
- `WithErrorHandler` - A function that is called when an error occurs.