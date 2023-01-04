package testserver

import (
	"context"
	"io"
	"net"
	"sync"
)

// The Handler type is allows clients to process incoming tcp connections.
// The provided context is canceled on Shutdown.
type Handler func(ctx context.Context, rwc io.ReadWriteCloser)

// A TestServer defines parameters for running an TCP server.
type TestServer struct {
	handler Handler

	mu      sync.Mutex
	wg      sync.WaitGroup
	closing bool

	l         net.Listener
	ctx       context.Context
	ctxCancel context.CancelFunc
}

func New(l net.Listener, h Handler) *TestServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &TestServer{
		handler:   h,
		l:         l,
		ctx:       ctx,
		ctxCancel: cancel,
	}
}

// Listen accepts incoming connections on the listener l, creating a new service goroutine for each message.
// The service goroutines read requests with a Decoder, then forward the request to the Handler and then
// write the response back to the client using an Encoder.
func (s *TestServer) Listen() {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return
		}

		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			conn.Close()
			return
		}
		s.wg.Add(1)
		s.mu.Unlock()

		go func(c net.Conn) {
			s.handler(s.ctx, c)
			s.wg.Done()
		}(conn)
	}
}

func (s *TestServer) Shutdown() {
	s.l.Close()

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.closing = true
	s.mu.Unlock()

	// Canceling context.
	s.ctxCancel()

	// Wait for active connections to close.
	s.wg.Wait()
}
