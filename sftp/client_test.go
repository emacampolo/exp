package sftp_test

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/emacampolo/exp/sftp"
	"github.com/emacampolo/exp/sftp/sftptest"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

var alphabet = []byte("abcdefghijklmnopqrstuvwxyz")

func TestNewClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Given
	ctx := context.Background()
	container, err := sftptest.NewContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer container.Stop(ctx)

	input := bytes.Repeat(alphabet, 100_000)
	reader := bytes.NewReader(input)

	sshConfig := &ssh.ClientConfig{
		User: "test",
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
		Auth: []ssh.AuthMethod{
			ssh.Password("test"),
		},
	}

	client, err := sftp.NewClient("tcp", container.NetworkAddress, sshConfig)
	if err != nil {
		t.Fatal(err)
	}

	// When
	err = client.Upload(reader, "/upload/alphabet.txt")
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	buf.Grow(len(input))
	err = client.Download("/upload/alphabet.txt", &buf)
	if err != nil {
		t.Fatal(err)
	}

	// Then
	require.Equal(t, input, buf.Bytes())
}
