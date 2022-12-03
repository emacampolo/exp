package sftp

import (
	"fmt"
	"io"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Client struct {
	delegate *sftp.Client
}

func NewClient(network string, addr string, config *ssh.ClientConfig) (*Client, error) {
	client, err := ssh.Dial(network, addr, config)
	if err != nil {
		return nil, err
	}

	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		return nil, err
	}

	return &Client{delegate: sftpClient}, nil
}

// Upload uploads the content of the reader to the target file on the remote server.
func (client *Client) Upload(local io.Reader, path string) error {
	remote, err := client.delegate.Create(path)
	if err != nil {
		return err
	}
	defer remote.Close()

	copied, err := io.Copy(remote, local)
	if err != nil {
		return err
	}

	// This method flushes the write buffer and waits for the server to acknowledge the receipt of the data.
	// This ensures that the data is sent to the server and that the server has received it.
	// However, it does not provide any guarantees that the data has been persisted on the server side
	// (for example, if the server crashes before the data is written to disk).
	if err := remote.Sync(); err != nil {
		return err
	}

	// Get the metadata for the file on the remote server
	info, err := client.delegate.Stat(path)
	if err != nil {
		return err
	}

	// Compare the size of the local file with the size of the remote file.
	// This is not a guarantee that the file is correct since the file could have been modified between the upload and the stat without the file size changing.
	// but it is a good enough check to ensure that the file was uploaded correctly.
	if info.Size() != copied {
		return fmt.Errorf("file size mismatch: %d != %d", info.Size(), copied)
	}

	return nil
}

// Download downloads the content of the source file on the remote server to the writer.
func (client *Client) Download(path string, writer io.Writer) error {
	source, err := client.delegate.Open(path)
	if err != nil {
		return err
	}
	defer source.Close()

	_, err = io.Copy(writer, source)
	return err
}

// Close closes the connection to the remote server.
func (client *Client) Close() error {
	return client.delegate.Close()
}
