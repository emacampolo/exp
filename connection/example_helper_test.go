package connection_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/emacampolo/exp/connection"
	"github.com/emacampolo/exp/connection/testserver"
	"github.com/moov-io/iso8583"
	"github.com/moov-io/iso8583/encoding"
	"github.com/moov-io/iso8583/field"
	"github.com/moov-io/iso8583/network"
	"github.com/moov-io/iso8583/prefix"
)

var testSpec = &iso8583.MessageSpec{
	Name: "ISO 8583 v1987 ASCII",
	Fields: map[int]field.Field{
		0: field.NewString(&field.Spec{
			Length:      4,
			Description: "Message Type Indicator",
			Enc:         encoding.ASCII,
			Pref:        prefix.ASCII.Fixed,
		}),
		1: field.NewBitmap(&field.Spec{
			Length:      8,
			Description: "Bitmap",
			Enc:         encoding.Binary,
			Pref:        prefix.Binary.Fixed,
		}),
		11: field.NewString(&field.Spec{
			Length:      6,
			Description: "Systems Trace Audit Number (STAN)",
			Enc:         encoding.ASCII,
			Pref:        prefix.ASCII.Fixed,
		}),
	},
}

type baseFields struct {
	MTI  *field.String `index:"0"`
	STAN *field.String `index:"11"`
}

// getSTAN returns a random string representing a Systems Trace Audit Number (STAN).
// The STAN is a 6-digit number that is used to uniquely identify a transaction.
func getSTAN() string {
	return fmt.Sprintf("%06d", rand.Intn(999999))
}

type iso8583Server struct {
	Addr   string
	Server *testserver.TestServer
}

func (t *iso8583Server) Shutdown() {
	t.Server.Shutdown()
}

func (t *iso8583Server) Handler() testserver.Handler {
	return func(ctx context.Context, rwc io.ReadWriteCloser) {
		defer rwc.Close()

		reader := bufio.NewReader(rwc)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			message, err := readISO8583(reader)
			if err != nil {
				log.Printf("error reading message: %v", err)
				return
			}

			if message == nil {
				return
			}

			// 0800 is the MTI for a sign on request.
			// 0200 is the MTI for a purchase request.
			// 0210 is the MTI for a purchase response.
			mti, err := message.GetMTI()
			if err != nil {
				log.Printf("error getting MTI: %v", err)
				return
			}

			if mti == "0800" {
				message.MTI("0810")
				// After sending the sign on response, we send a purchase request
				// to the client in a separate goroutine.
				go func() {
					time.Sleep(100 * time.Millisecond)

					purchaseRequest := iso8583.NewMessage(testSpec)
					purchaseRequest.MTI("0200")
					purchaseRequest.Field(11, "123456")
					if err := writeISO8583(rwc, purchaseRequest); err != nil {
						log.Printf("error writing purchase request: %v", err)
					}
				}()
			} else if mti == "0210" {
				stan, err := message.GetString(11)
				if err != nil {
					log.Printf("error getting STAN: %v", err)
					return
				}
				fmt.Println("received purchase response with STAN:", stan)
				return
			} else {
				log.Printf("unknown MTI: %v", mti)
				return
			}

			if err := writeISO8583(rwc, message); err != nil {
				log.Printf("error replying to message: %v", err)
				return
			}
		}
	}
}

func readMessageLength(r io.Reader) (int, error) {
	header := network.NewBinary2BytesHeader()
	n, err := header.ReadFrom(r)
	if err != nil {
		return n, err
	}

	return header.Length(), nil
}

func readISO8583(reader io.Reader) (*iso8583.Message, error) {
	length, err := readMessageLength(reader)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		log.Printf("server: error reading message: %v", err)
		return nil, err
	}

	rawMessage := make([]byte, length)
	_, err = io.ReadFull(reader, rawMessage)
	if err != nil {
		return nil, fmt.Errorf("reading message: %w", err)
	}

	message := iso8583.NewMessage(testSpec)
	if err := message.Unpack(rawMessage); err != nil {
		return nil, fmt.Errorf("unpacking message: %w", err)
	}

	return message, nil
}

func writeISO8583(writer io.Writer, message *iso8583.Message) error {
	var buf bytes.Buffer
	packed, err := message.Pack()
	if err != nil {
		return fmt.Errorf("packing message: %w", err)
	}

	_, err = writeMessageLength(&buf, len(packed))
	if err != nil {
		return fmt.Errorf("writing message header to buffer: %w", err)
	}

	_, err = buf.Write(packed)
	if err != nil {
		return fmt.Errorf("writing packed message to buffer: %w", err)
	}

	_, err = writer.Write(buf.Bytes())
	return err
}

func writeMessageLength(w io.Writer, length int) (int, error) {
	header := network.NewBinary2BytesHeader()
	header.SetLength(length)

	n, err := header.WriteTo(w)
	if err != nil {
		return n, fmt.Errorf("writing message header: %w", err)
	}

	return n, nil
}

func newISO8583ServerWithAddr(addr string) (*iso8583Server, error) {
	testSrv := &iso8583Server{}
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

func newISO8583Server() (*iso8583Server, error) {
	return newISO8583ServerWithAddr("127.0.0.1:")
}

type iso8583EncodeDecoder struct {
	buff *bufio.Reader
	once sync.Once
}

func (ed *iso8583EncodeDecoder) Encode(writer io.Writer, message []byte) error {
	var buf bytes.Buffer
	_, err := writeMessageLength(&buf, len(message))
	if err != nil {
		return fmt.Errorf("writing message header to buffer: %w", err)
	}

	_, err = buf.Write(message)
	if err != nil {
		return fmt.Errorf("writing packed message to buffer: %w", err)
	}

	_, err = writer.Write(buf.Bytes())
	return err
}

func (ed *iso8583EncodeDecoder) Decode(reader io.Reader) (b []byte, err error) {
	ed.once.Do(func() {
		ed.buff = bufio.NewReader(reader)
	})

	length, err := readMessageLength(ed.buff)
	if err != nil {
		return nil, fmt.Errorf("reading message length: %w", err)
	}

	rawMessage := make([]byte, length)
	_, err = io.ReadFull(ed.buff, rawMessage)
	if err != nil {
		return nil, fmt.Errorf("reading message: %w", err)
	}

	return rawMessage, nil
}

type iso8583MarshalUnmarshal struct{}

func (iso8583MarshalUnmarshal) Marshal(message connection.Message) ([]byte, error) {
	return message.Payload.(*iso8583.Message).Pack()
}

func (iso8583MarshalUnmarshal) Unmarshal(data []byte) (connection.Message, error) {
	message := iso8583.NewMessage(testSpec)
	if err := message.Unpack(data); err != nil {
		return connection.Message{}, fmt.Errorf("unpacking message: %w", err)
	}

	id, err := message.GetString(11)
	if err != nil {
		return connection.Message{}, fmt.Errorf("getting message id: %w", err)
	}

	return connection.Message{ID: id, Payload: message}, nil
}
