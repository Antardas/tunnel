package client

import (
	"antardas/tcp-tunnel/protocol"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
)

var activeClientId string

func Connect(clientId string) {
	for {
		err := connectOnce(clientId)
		if err != nil {
			fmt.Println("Tunnel Disconnected: ", err)
		}
		fmt.Println("Reconnecting in 2 seconds...")
		time.Sleep(2 * time.Second)
	}
}

func connectOnce(clientId string) error {
	fmt.Println("Connecting to the public server")

	conn, err := net.DialTimeout("tcp", "tunnel.oshudstock.com:5135", 5*time.Second)

	if err != nil {
		return err
	}

	defer conn.Close()
	meta := protocol.Metadata{
		ID:       uuid.NewString(),
		ClientId: clientId,
		Type:     "init",
		Path:     "",
		Method:   "",
	}
	protocol.WriteMessage(conn, meta, bytes.NewReader([]byte("")), 0)
	fmt.Println("Conneted to the server")
	activeClientId = clientId

	for {

		meta, body, err := protocol.ReadMessage(conn)
		if err != nil {
			fmt.Println("Reading Request failed:", err)
			continue
		}

		if meta.Type != "request" {
			fmt.Println("Unexpected message type:", meta.Type)
			continue
		}

		if err := callService(conn, meta, body); err != nil {
			fmt.Println("Reading Request failed:", err)
			continue
		}

	}
}

func callService(conn net.Conn, meta protocol.Metadata, rawBody []byte) error {
	// here this URL from server TCP metadata
	if meta.ClientHost == "" {
		return errors.New("clientHost is required")
	}
	if meta.ClientPort == "" {
		return errors.New("clientPort is required")
	}
	path := fmt.Sprintf("http://localhost:%s", meta.ClientPort) + meta.Path
	if meta.RawQuery != "" {
		path += "?" + meta.RawQuery
	}
	fmt.Printf("Path: %s\n", path)
	req, err := http.NewRequest(meta.Method, path, bytes.NewReader(rawBody))
	if err != nil {
		fmt.Println("Error happend during calling local service")
		return err
	}
	req.Header = meta.Headers.Clone()
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	fmt.Printf("Status: %d\n", resp.StatusCode)
	fmt.Printf("Body: %d\n", len(body))
	responseMeta := protocol.Metadata{
		ID:         meta.ID,
		ClientId:   activeClientId,
		Type:       "response",
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
	}

	return protocol.WriteMessage(conn, responseMeta, bytes.NewReader(body), uint32(len(body)))
}
