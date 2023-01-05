package connection_test

import (
	"log"
	"time"

	"github.com/emacampolo/exp/connection"
	"github.com/moov-io/iso8583"
	"github.com/moov-io/iso8583/field"
)

// Example demonstrates how to use the connection package to send and receive ISO8583 messages.
// The connection will act as a client signing on to a fake VISA server.
// The flow will be as follows:
// 1. Client sends a sign on request to the server.
// 2. Server responds with a sign on response and after a fixed delay sends an authorization request.
// 3. Client responds with an authorization response.
func Example() {
	server, err := newISO8583Server()
	if err != nil {
		log.Fatalf("error creating test server: %v", err)
	}
	defer server.Shutdown()

	handler := func(connection *connection.Connection, message connection.Message) {
		m := message.Payload.(*iso8583.Message)
		m.MTI("0210")
		if err := connection.Reply(message); err != nil {
			log.Fatalf("error replying to message: %v", err)
		}
	}

	conn, err := connection.New("tcp", server.Addr, &iso8583EncodeDecoder{}, iso8583MarshalUnmarshal{}, handler)
	if err != nil {
		log.Fatalf("error creating connection: %v", err)
	}

	if err := conn.Connect(); err != nil {
		log.Fatalf("error connecting: %v", err)
	}

	defer conn.Close()

	message := iso8583.NewMessage(testSpec)
	id := getSTAN()
	err = message.Marshal(baseFields{
		MTI:  field.NewStringValue("0800"),
		STAN: field.NewStringValue(id),
	})

	if err != nil {
		log.Fatalf("error marshaling message: %v", err)
	}

	response, err := conn.Send(connection.Message{ID: id, Payload: message})
	if err != nil {
		log.Fatalf("error sending message: %v", err)
	}

	responseMessage := response.Payload.(*iso8583.Message)
	responseMTI, err := responseMessage.GetMTI()
	if err != nil {
		log.Fatalf("error getting response MTI: %v", err)
	}

	if responseMTI != "0810" {
		log.Fatalf("expected response MTI to be 0810, got %s", responseMTI)
	}

	// After the sign on response is received, we wait for the server to send a purchase request.
	time.Sleep(200 * time.Millisecond)

	// Output: received purchase response with STAN: 123456
}