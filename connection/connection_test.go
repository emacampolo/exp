package connection_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"exp/connection"
	"exp/connection/testserver"
)

type testServer struct {
	Addr   string
	Server *testserver.TestServer
}

func (t *testServer) Shutdown() {
	t.Server.Shutdown()
}

func (t *testServer) Handler() testserver.Handler {
	return func(ctx context.Context, conn net.Conn) {
		reader := bufio.NewReader(conn)
		for {
			id, payload := read(reader)
			if payload == "" {
				return
			}

			switch payload {
			case "ping":
				log.Println("server: received ping, sending pong")
				conn.Write([]byte(fmt.Sprintf("%s,pong\n", id)))
			case "sign_on":
				log.Println("server: received sign_on, sending ack")
				conn.Write([]byte(fmt.Sprintf("%s,signed_on\n", id)))

				for i := 0; i < 1000; i++ {
					randomString := make([]byte, 10)
					rand.Read(randomString)
					// ID starts from 100 to avoid collision with other messages.
					_, err := conn.Write([]byte(fmt.Sprintf("%d,%s\n", i+100, hex.EncodeToString(randomString))))
					if err != nil {
						log.Println("server: error writing message:", err)
						return
					}
				}
				log.Println("server: finished writing messages")
			case "sign_off":
				conn.Write([]byte(fmt.Sprintf("%s,signed_off\n", id)))
				log.Println("server: sign-off sent")
				// Wait for the client to decode the ack before closing the connection since
				// the read loop is still running and will try to read from the connection and
				// will get an EOF error before the client has a chance to decode the ack.
				time.Sleep(100 * time.Millisecond)
				return
			case "delay":
				log.Println("server: received delay")
				time.Sleep(200 * time.Millisecond)
				conn.Write([]byte(fmt.Sprintf("%s,ack\n", id)))
			default:
				log.Println("server: received unknown message:", payload)
				return
			}
		}
	}
}

func newTestServerWithAddr(addr string) (*testServer, error) {
	testSrv := &testServer{}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	srv := testserver.New(ln, testSrv.Handler())
	testSrv.Server = srv
	testSrv.Addr = ln.Addr().String()

	go srv.Listen()

	return testSrv, nil
}

func read(reader *bufio.Reader) (string, string) {
	message, err := reader.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			log.Println("server: read EOF, closing connection")
			return "", ""
		}
		log.Printf("server: error reading message: %v", err)
		return "", ""
	}

	// Strip the newline.
	message = strings.TrimSpace(message)

	id, payload, found := strings.Cut(message, ",")
	if !found {
		log.Printf("server: error parsing message: %q", message)
		return "", ""
	}
	return id, payload
}

func newTestServer() (*testServer, error) {
	return newTestServerWithAddr("127.0.0.1:")
}

// encodeDecoder implements a connection.EncodeDecoder that uses a new line as a delimiter for messages.
type encodeDecoder struct {
	buff *bufio.Reader
	once sync.Once
}

func (ed *encodeDecoder) Encode(writer io.Writer, message []byte) error {
	writer.Write(message)
	writer.Write([]byte("\n"))
	return nil
}

func (ed *encodeDecoder) Decode(reader io.Reader) (b []byte, err error) {
	ed.once.Do(func() {
		ed.buff = bufio.NewReader(reader)
	})

	b, err = ed.buff.ReadBytes('\n')
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
		return connection.Message{}, fmt.Errorf("error unmarshaling message: could not find comma: %q", data)
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

		c, err := connection.New("tcp", server.Addr, &encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicErrorHandler)

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
		c, err := connection.New("tcp", "10.0.0.0:50000", &encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicErrorHandler, connection.WithDialTimeout(2*time.Second))
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
		c, err := connection.New("tcp", "", &encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicErrorHandler, connection.WithDialTimeout(2*time.Second))
		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		if err := c.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}
	})
}

func TestClient_Send(t *testing.T) {
	t.Run("send messages to server and receives responses", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		c, err := connection.New("tcp", server.Addr, &encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, alwaysPanicErrorHandler)
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
	t.Run("send sign on message to server and wait for all messages to be handled", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		var count atomic.Int64
		handler := func(connection *connection.Connection, message connection.Message) {
			// Simulate a long running task.
			count.Add(1)
			time.Sleep(1 * time.Second)
		}

		c, err := connection.New("tcp", server.Addr, &encodeDecoder{}, marshalUnmarshal{}, handler, func(err error) {
			if !errors.Is(err, io.EOF) {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		if err := c.Connect(); err != nil {
			t.Fatalf("error connecting: %v", err)
		}

		result, err := c.Send(connection.Message{ID: "1", Payload: "sign_on"})
		if err != nil {
			t.Fatalf("error sending message: %v", err)
		}

		if result.Payload.(string) != "signed_on" {
			t.Fatalf("result: %v, expected signed_on", result)
		}

		log.Println("waiting for all messages to be handled")
		time.Sleep(1 * time.Second)

		result, err = c.Send(connection.Message{ID: "1", Payload: "sign_off"})
		if err != nil {
			t.Fatalf("error sending message: %v", err)
		}

		if result.Payload.(string) != "signed_off" {
			t.Fatalf("result: %v, expected signed_off", result)
		}

		if err := c.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}

		if count.Load() != 1000 {
			t.Fatalf("expected 1000 messages to be handled, got %v", count.Load())
		}
	})
	t.Run("return ErrConnectionClosed when Close was called", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		c, err := connection.New("tcp", server.Addr, &encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, func(err error) {
			if !errors.Is(err, io.EOF) {
				t.Errorf("unexpected error: %v", err)
			}
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

		_, err = c.Send(connection.Message{ID: "1", Payload: "sign_on"})
		if !errors.Is(err, connection.ErrConnectionClosed) {
			t.Fatalf("expected ErrConnectionClosed, got %v", err)
		}
	})
	t.Run("return ErrSendTimeout when response was not received during SendTimeout time", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		c, err := connection.New("tcp", server.Addr, &encodeDecoder{}, marshalUnmarshal{}, alwaysPanicHandler, func(err error) {
			if !errors.Is(err, io.EOF) {
				t.Errorf("unexpected error: %v", err)
			}
		}, connection.WithSendTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		if err := c.Connect(); err != nil {
			t.Fatalf("error connecting: %v", err)
		}

		defer c.Close()

		_, err = c.Send(connection.Message{ID: "1", Payload: "delay"})
		if !errors.Is(err, connection.ErrSendTimeout) {
			t.Fatalf("expected ErrSendTimeout, got %v", err)
		}
	})
}
