# Package connection

This package is heavily inspired by https://github.com/moov-io/iso8583-connection.
The key differences are:

- The messaging protocol is not tied to ISO8583 or the usage of [moov-io/iso8583](https://github.com/moov-io/iso8583).
- It provides a higher level abstraction for sending and receiving messages over a connection.
- It ensures that when Close is called, the function will only return once all messages sent using `Send` or `Reply`
  have been processed, or any `Message` forwarded to `InboundMessageHandler` has finished being handled.

For an example of how to use this package, see a [testable example](./example_test.go).