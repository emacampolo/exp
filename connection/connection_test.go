package connection_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

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

			// Strip the newline.
			message = strings.TrimSpace(message)

			id, payload, found := strings.Cut(message, ",")
			if !found {
				fmt.Printf("error parsing message: %v", err)
				return
			}

			if payload == "ping" {
				conn.Write([]byte(fmt.Sprintf("%s,pong\n", id)))
			} else {
				conn.Write([]byte("unknown\n"))
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
	b, err := bufio.NewReader(reader).ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	// Strip the newline.
	b = bytes.TrimSpace(b)
	return b, nil
}

// marshalUnmarshal implements a connection.MarshalUnmarshaler that assume that the connection.Message payload is a string.
type marshalUnmarshal struct{}

func (marshalUnmarshal) Marshal(message connection.Message) ([]byte, error) {
	return []byte(fmt.Sprintf("%s,%s", message.ID, message.Payload)), nil
}

func (marshalUnmarshal) Unmarshal(data []byte) (connection.Message, error) {
	id, payload, found := bytes.Cut(data, []byte(","))
	if !found {
		return connection.Message{}, fmt.Errorf("error parsing message: could not find comma: %q", data)
	}

	return connection.Message{ID: string(id), Payload: string(payload)}, nil
}

var alwaysPanicHandler = func(c *connection.Connection, message connection.Message) {
	panic("always fail")
}

var alwaysPanicErrorHandler = func(err error) {
	panic(err)
}

func TestClient_Connect(t *testing.T) {
	t.Run("connect", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		c, err := connection.New("tcp", server.Addr, encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicErrorHandler)

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
	t.Run("connect times out", func(t *testing.T) {
		c, err := connection.New("tcp", "10.0.0.0:50000", encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicErrorHandler, connection.WithDialTimeout(2*time.Second))
		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		start := time.Now()
		err = c.Connect()
		end := time.Now()
		delta := end.Sub(start)

		if err == nil {
			t.Fatalf("expected error, got nil")
		}

		// Test against triple the timeout value to be safe, which should also be well under any OS specific socket timeout.
		// Realistically, the delta should nearly always be exactly 2 seconds.
		if delta < 2*time.Second || delta > 6*time.Second {
			t.Fatalf("expected timeout to be between 2 and 6 seconds, got %v", delta)
		}

		if err := c.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}
	})
	t.Run("no panic when Close before Connect", func(t *testing.T) {
		c, err := connection.New("tcp", "", encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicErrorHandler, connection.WithDialTimeout(2*time.Second))
		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		if err := c.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}
	})
}

func TestClient_Send(t *testing.T) {
	server, err := newTestServer()
	if err != nil {
		t.Fatalf("error creating test server: %v", err)
	}
	defer server.Shutdown()

	t.Run("sends messages to server and receives responses", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		c, err := connection.New("tcp", server.Addr, encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicErrorHandler)
		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		if err := c.Connect(); err != nil {
			t.Fatalf("error connecting: %v", err)
		}

		result, err := c.Send(connection.Message{ID: "1", Payload: "ping"})
		if err != nil {
			t.Fatalf("error sending message: %v", err)
		}

		if result.Payload.(string) != "pong" {
			t.Fatalf("unexpected result: %v", result)
		}

		if err := c.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}
	})
}
