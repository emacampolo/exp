package sftptest

import (
	"context"
	"fmt"
	"log"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Container encapsulates all the setup required for creating a container
// for running integration tests that need a sftp server.
type Container struct {
	container testcontainers.Container

	// NetworkAddress for connecting to the container using a driver.
	NetworkAddress string
}

type LogConsumer struct{}

func (g *LogConsumer) Accept(l testcontainers.Log) {
	log.Print(string(l.Content))
}

// NewContainer returns a sftp container ready to be used.
// Callers must call Stop when it is no longer needed.
func NewContainer(ctx context.Context) (*Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "atmoz/sftp",
		ExposedPorts: []string{"22/tcp"},
		// <username>:<password>:<uid>:<gid>:<directory>
		Cmd:        []string{"test:test:1001:100:upload"},
		WaitingFor: wait.ForListeningPort("22/tcp"),
	}

	sftpServer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return nil, err
	}

	err = sftpServer.StartLogProducer(ctx)
	if err != nil {
		return nil, err
	}

	sftpServer.FollowOutput(&LogConsumer{})

	host, err := sftpServer.Host(ctx)
	if err != nil {
		return nil, err
	}

	p, err := sftpServer.MappedPort(ctx, "22/tcp")
	if err != nil {
		return nil, err
	}

	port := p.Int()

	return &Container{
		container:      sftpServer,
		NetworkAddress: fmt.Sprintf("%s:%d", host, port),
	}, nil
}

// Stop stops the container, cleaning up resources.
func (c *Container) Stop(ctx context.Context) error {
	return c.container.Terminate(ctx)
}
