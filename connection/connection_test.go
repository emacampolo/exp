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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emacampolo/exp/connection"
	"github.com/emacampolo/exp/connection/testserver"
)

type testServer struct {
	Addr   string
	Server *testserver.TestServer

	mutex         sync.Mutex
	receivedPings int
}

func (t *testServer) IncrementPings() {
	t.mutex.Lock()
	t.receivedPings++
	t.mutex.Unlock()
}

func (t *testServer) ReceivedPings() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.receivedPings
}

func (t *testServer) Shutdown() {
	t.Server.Shutdown()
}

func (t *testServer) Handler() testserver.Handler {
	return func(ctx context.Context, rwc io.ReadWriteCloser) {
		defer rwc.Close()
		reader := bufio.NewReader(rwc)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			id, payload := read(reader)
			if payload == "" {
				return
			}

			go func() {
				switch payload {
				case "ping":
					log.Println("server: received ping")
					t.IncrementPings()
					rwc.Write([]byte(fmt.Sprintf("%s,pong\n", id)))
				case "sign_on":
					rwc.Write([]byte(fmt.Sprintf("%s,signed_on\n", id)))

					for i := 0; i < 1000; i++ {
						randomString := make([]byte, 10)
						rand.Read(randomString)
						// ID starts from 100 to avoid collision with other messages.
						_, err := rwc.Write([]byte(fmt.Sprintf("%d,%s\n", i+100, hex.EncodeToString(randomString))))
						if err != nil {
							log.Println("server: error writing message:", err)
							return
						}
					}
					log.Println("server: finished writing messages")
				case "sign_off":
					rwc.Write([]byte(fmt.Sprintf("%s,signed_off\n", id)))
					// Wait for the client to decode the ack before closing the connection since
					// the read loop is still running and will try to read from the connection and
					// will get an EOF error before the client has a chance to decode the ack.
					time.Sleep(100 * time.Millisecond)
					return
				case "delay":
					time.Sleep(500 * time.Millisecond)
					rwc.Write([]byte(fmt.Sprintf("%s,ack\n", id)))
				default:
					log.Println("server: received unknown message:", payload)
					return
				}
			}()
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
	return newTestServerWithAddr("127.0.0.1:0")
}

// encodeDecoder implements a connection.EncodeDecoder that uses a new line as a delimiter for messages.
type encodeDecoder struct{}

func (ed encodeDecoder) Encode(writer io.Writer, message []byte) error {
	writer.Write(message)
	writer.Write([]byte("\n"))
	return nil
}

func (ed encodeDecoder) Decode(reader io.Reader) ([]byte, error) {
	return readLine(reader)
}

// readLine reads a line from the reader until it encounters a new line character.
// The new line is not included in the returned message.
// We should not read more bytes than required to find the new line since the reader is a stream.
// If we read more bytes than required, the next call to Decode will read from the same stream and will read the bytes that we read here.
// This will cause the message to be corrupted.
func readLine(reader io.Reader) ([]byte, error) {
	var message []byte
	for {
		b := make([]byte, 1)
		_, err := reader.Read(b)
		if err != nil {
			return nil, err
		}

		if b[0] == '\n' {
			break
		}

		message = append(message, b[0])
	}
	return message, nil
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

var alwaysPanicHandler = func(c connection.Connection, message connection.Message) {
	panic("always fail")
}

func TestConnection_Connect(t *testing.T) {
	t.Run("connect", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		conn, err := connection.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			connection.NewOptions(),
		)

		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		if err := conn.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}
	})
}

func TestConnection_Send(t *testing.T) {
	t.Run("send messages to server and receives responses", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		conn, err := connection.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			connection.NewOptions(),
		)

		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		result, err := conn.Request(connection.Message{ID: "1", Payload: "ping"})
		if err != nil {
			t.Fatalf("error sending message: %v", err)
		}

		if result.Payload.(string) != "pong" {
			t.Fatalf("unexpected result: %v", result)
		}

		if err := conn.Close(); err != nil {
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
		handler := func(connection connection.Connection, message connection.Message) {
			// Simulate a long running task.
			count.Add(1)
			time.Sleep(1 * time.Second)
		}

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := connection.NewOptions()
		options.SetErrorHandler(func(err error) {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		conn, err := connection.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			handler,
			options,
		)

		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		result, err := conn.Request(connection.Message{ID: "1", Payload: "sign_on"})
		if err != nil {
			t.Fatalf("error sending message: %v", err)
		}

		if result.Payload.(string) != "signed_on" {
			t.Fatalf("result: %v, expected signed_on", result)
		}

		log.Println("waiting for all messages to be handled")
		time.Sleep(1 * time.Second)

		result, err = conn.Request(connection.Message{ID: "1", Payload: "sign_off"})
		if err != nil {
			t.Fatalf("error sending message: %v", err)
		}

		if result.Payload.(string) != "signed_off" {
			t.Fatalf("result: %v, expected signed_off", result)
		}

		if err := conn.Close(); err != nil {
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

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := connection.NewOptions()
		options.SetErrorHandler(func(err error) {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		conn, err := connection.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			options,
		)

		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		if err := conn.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}

		_, err = conn.Request(connection.Message{ID: "1", Payload: "sign_on"})
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

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := connection.NewOptions()
		if err := options.SetRequestTimeout(100 * time.Millisecond); err != nil {
			t.Fatalf("error setting request timeout: %v", err)
		}
		options.SetErrorHandler(func(err error) {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		conn, err := connection.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			options,
		)

		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		defer conn.Close()

		_, err = conn.Request(connection.Message{ID: "1", Payload: "delay"})
		if !errors.Is(err, connection.ErrRequestTimeout) {
			t.Fatalf("expected ErrRequestTimeout, got %v", err)
		}
	})
	t.Run("return error when the Message does not have an ID", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := connection.NewOptions()
		if err := options.SetRequestTimeout(100 * time.Millisecond); err != nil {
			t.Fatalf("error setting request timeout: %v", err)
		}
		options.SetErrorHandler(func(err error) {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		conn, err := connection.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			options,
		)

		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		defer conn.Close()

		_, err = conn.Request(connection.Message{Payload: "delay"})
		if err == nil || err.Error() != "message ID is empty" {
			t.Fatalf("expected error with \"message ID is empty\", got %q", err.Error())
		}
	})
	t.Run("pending requests should complete after Close was called", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		conn, err := connection.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			connection.NewOptions(),
		)

		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		var wg sync.WaitGroup
		var errs []error
		var mu sync.Mutex

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer func() {
					wg.Done()
				}()

				id := fmt.Sprintf("%v", i+1)
				result, err := conn.Request(connection.Message{ID: id, Payload: "delay"})
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("error sending message: %v", err))
					mu.Unlock()
					return
				}

				if result.Payload.(string) != "ack" {
					mu.Lock()
					errs = append(errs, fmt.Errorf("result: %v, expected \"ack\"", result))
					mu.Unlock()
					return
				}
			}(i)
		}

		if len(errs) > 0 {
			t.Fatalf("unexpected errors: %v", errs)
		}

		// Let's wait all messages to be sent
		time.Sleep(100 * time.Millisecond)
		if err := conn.Close(); err != nil {
			t.Fatalf("error closing connection: %v", err)
		}
		wg.Wait()
	})
	t.Run("responses received asynchronously", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		conn, err := connection.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			connection.NewOptions(),
		)

		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		defer conn.Close()

		var (
			results []connection.Message
			wg      sync.WaitGroup
			mu      sync.Mutex
			errs    []error
		)

		wg.Add(1)
		go func() {
			defer wg.Done()

			result, err := conn.Request(connection.Message{ID: "1", Payload: "delay"})
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("error sending message 1: %v", err))
				mu.Unlock()
				return
			}

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			// This message will be sent after the first one
			time.Sleep(100 * time.Millisecond)

			result, err := conn.Request(connection.Message{ID: "2", Payload: "ping"})
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("error sending message 2: %v", err))
				mu.Unlock()
				return
			}

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}()

		// Let's wait all messages to be sent
		time.Sleep(200 * time.Millisecond)
		wg.Wait()

		if len(errs) > 0 {
			t.Fatalf("unexpected errors: %v", errs)
		}

		if len(results) != 2 {
			t.Fatalf("unexpected results: %v", results)
		}

		// We expect that response for the second message was received first.
		if results[0].ID != "2" {
			t.Fatalf("expected result with ID \"2\", got %q", results[0].ID)
		}

		if results[1].ID != "1" {
			t.Fatalf("expected result with ID \"1\", got %q", results[1].ID)
		}
	})
	t.Run("automatically sends ping messages after write timeout interval", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		pingHandler := func(c connection.Connection) {
			// Generate random ID
			id := fmt.Sprintf("%v", rand.Int63())
			result, err := c.Request(connection.Message{ID: id, Payload: "ping"})
			if err != nil {
				if errors.Is(err, connection.ErrConnectionClosed) {
					return
				}

				t.Fatalf("error sending message: %v", err)
			}

			if result.Payload.(string) != "pong" {
				t.Fatalf("result: %v, expected signed_on", result)
			}
		}

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := connection.NewOptions()
		if err := options.SetWriteTimeoutFunc(25*time.Millisecond, pingHandler); err != nil {
			t.Fatalf("error setting write timeout: %v", err)
		}

		conn, err := connection.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			alwaysPanicHandler,
			options,
		)

		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		defer conn.Close()

		// We expect that ping interval in 50ms has not passed yet
		// and server has not being pinged yet.
		if server.ReceivedPings() != 0 {
			t.Fatalf("expected 0 received pings, got %v", server.receivedPings)
		}

		time.Sleep(200 * time.Millisecond)

		// On average, we expect that server has been pinged 7 times.
		// We use 5 as a threshold to avoid flaky tests.
		if server.ReceivedPings() < 5 {
			t.Fatalf("expected at least 3 received ping, got %v", server.receivedPings)
		}
	})
	t.Run("ignores forgotten messages", func(t *testing.T) {
		server, err := newTestServer()
		if err != nil {
			t.Fatalf("error creating test server: %v", err)
		}
		defer server.Shutdown()

		netConn, err := net.Dial("tcp", server.Addr)
		if err != nil {
			t.Fatalf("error dialing test server: %v", err)
		}

		options := connection.NewOptions()
		if err := options.SetRequestTimeout(500 * time.Millisecond); err != nil {
			t.Fatalf("error setting request timeout: %v", err)
		}
		options.SetErrorHandler(func(err error) {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		var forgottenMessage connection.Message
		handler := func(c connection.Connection, message connection.Message) {
			forgottenMessage = message
		}

		conn, err := connection.New(
			netConn,
			encodeDecoder{},
			marshalUnmarshal{},
			handler,
			options,
		)

		if err != nil {
			t.Fatalf("error creating connection: %v", err)
		}

		defer conn.Close()

		_, err = conn.Request(connection.Message{ID: "1", Payload: "delay"})
		if !errors.Is(err, connection.ErrRequestTimeout) {
			t.Fatalf("expected ErrRequestTimeout, got %v", err)
		}

		time.Sleep(1 * time.Second)
		if forgottenMessage.ID == "1" {
			t.Fatalf("expected forgotten message to be empty, got %v", forgottenMessage)
		}
	})
}

func BenchmarkSend100(b *testing.B) { benchmarkSend(100, b) }

func BenchmarkSend1000(b *testing.B) { benchmarkSend(1000, b) }

func BenchmarkSend10000(b *testing.B) { benchmarkSend(10000, b) }

func BenchmarkSend100000(b *testing.B) { benchmarkSend(100000, b) }

func benchmarkSend(m int, b *testing.B) {
	server, err := newTestServer()
	if err != nil {
		b.Fatalf("error creating test server: %v", err)
	}
	defer server.Shutdown()

	netConn, err := net.Dial("tcp", server.Addr)
	if err != nil {
		b.Fatalf("error dialing test server: %v", err)
	}

	options := connection.NewOptions()
	options.SetErrorHandler(func(err error) {
		if !errors.Is(err, io.EOF) {
			b.Fatalf("unexpected error: %v", err)
		}
	})

	conn, err := connection.New(
		netConn,
		encodeDecoder{},
		marshalUnmarshal{},
		alwaysPanicHandler,
		options,
	)

	if err != nil {
		b.Fatalf("error creating connection: %v", err)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		processMessages(b, m, conn)
	}

	err = conn.Close()
	if err != nil {
		b.Fatal("closing client: ", err)
	}
}

// send/receive m messages
func processMessages(b *testing.B, m int, conn connection.Connection) {
	var wg sync.WaitGroup
	var gerr error

	for i := 0; i < m; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			result, err := conn.Request(connection.Message{ID: strconv.Itoa(i), Payload: "ping"})
			if err != nil {
				gerr = err
				return
			}

			if result.Payload.(string) != "pong" {
				gerr = err
				return
			}
		}(i)
	}

	wg.Wait()
	if gerr != nil {
		b.Fatal("sending message: ", gerr)
	}
}
