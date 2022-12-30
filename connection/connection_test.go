package connection_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"exp/connection"
	"exp/connection/testserver"
)

type testServer struct {
	Addr string

	Server *testserver.TestServer
}

func (t *testServer) Shutdown() {
	t.Server.Shutdown()
}

func newTestServerWithAddr(addr string) (*testServer, error) {
	var testSrv *testServer
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	srv := testserver.New(ln, func(ctx context.Context, conn net.Conn) {
		for {
			message, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil {
				fmt.Printf("error reading message: %v", err)
				return
			}

			if message == "ping\n" {
				conn.Write([]byte("pong\n"))
			}
		}
	})

	go srv.Listen()

	testSrv = &testServer{
		Server: srv,
		Addr:   ln.Addr().String(),
	}

	return testSrv, nil
}

func newTestServer() (*testServer, error) {
	return newTestServerWithAddr("127.0.0.1:")
}

// encodeDecoder implements a connection.EncodeDecoder that uses a new line as a delimiter for messages.
type encodeDecoder struct{}

func (encodeDecoder) Encode(writer io.Writer, message []byte) error {
	writer.Write(message)
	writer.Write([]byte("\n"))
	return nil
}

func (encodeDecoder) Decode(reader io.Reader) ([]byte, error) {
	return bufio.NewReader(reader).ReadBytes('\n')
}

// marshalUnmarshal implements a connection.MarshalUnmarshaler that assume that the connection.Message payload is a string.
type marshalUnmarshal struct{}

func (marshalUnmarshal) Marshal(message connection.Message) ([]byte, error) {
	return []byte(message.Payload.(string)), nil
}

func (marshalUnmarshal) Unmarshal(data []byte) (connection.Message, error) {
	return connection.Message{Payload: string(data)}, nil
}

func TestClient_Connect(t *testing.T) {
	t.Run("connect", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		handler := func(c *connection.Connection, message connection.Message) {
			result, err := c.Send(connection.Message{Payload: "ping"})
			if err != nil {
				t.Fatalf("error sending message: %v", err)
			}

			if result.Payload != "pong" {
				t.Fatalf("unexpected result: %v", result)
			}
		}

		c, err := connection.New("tcp", server.Addr, encodeDecoder{}, marshalUnmarshal{}, handler, func(err error) {
			t.Fatalf("handler error: %v", err)
		})

		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		if err := c.Connect(); err != nil {
			t.Fatalf("error connecting: %v", err)
		}

		if err := c.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}
	})
}
