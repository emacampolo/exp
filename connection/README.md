# Package connection

This package is heavily inspired by https://github.com/moov-io/iso8583-connection.
The key differences are:

- The messaging protocol is not tied to ISO8583 or the usage of [moov-io/iso8583](https://github.com/moov-io/iso8583).
- It Provides a higher level abstraction for sending and receiving messages over a connection.
- It ensures that when Close is called, the function will only return once all messages sent using `Send` or `Reply`
  have been processed, or any `Message` forwarded to `InboundMessageHandler` has finished being handled.

## Example

To run a working example, see [example_test.go](./example_test.go).

## Options

The following options are available when creating a new connection:

- `WithWriteTimeoutFunc` - A function that is called when there is no message sent to the server within a given
  duration.
- `WithReadTimeoutFunc` - A function that is called when there is no message received from the server within a given
  duration.
- `WithSendTimeout` - The duration to wait for a response from the server when sending a `Message`.
- `WithDialTimeout` - The duration to wait for a connection to be established.
- `WithErrorHandler` - A function that is called when an error occurs.