package client

import (
	"antardas/tcp-tunnel/protocol"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

func Connect() {
	for {
		err := connectOnce()
		if err != nil {
			fmt.Println("Tunnel Disconnected: ", err)
		}
		fmt.Println("Reconnecting in 2 seconds...")
		time.Sleep(2 * time.Second)
	}
}

func connectOnce() error {
	fmt.Println("Connecting to the public server")

	conn, err := net.DialTimeout("tcp", "localhost:8080", 5*time.Second)

	if err != nil {
		return err
	}

	defer conn.Close()

	fmt.Println("Conneted to the server")

	for {

		meta, body, err := protocol.ReadMessage(conn)

		if err != nil {
			return err

		}

		if meta.Type != "request" {
			fmt.Println("Unexpected message type:", meta.Type)
			continue
		}

		if err := callService(conn, meta, body); err != nil {
			return err
		}

	}
}

func callService(conn net.Conn, meta protocol.Metadata, rawBody []byte) error {
	// here this URL from server TCP metadata
	path := "http://localhost:4050" + meta.Path
	if meta.RawQuery != "" {
		path += "?" + meta.RawQuery
	}
	fmt.Printf("Path: %s\n", path)
	req, err := http.NewRequest(meta.Method, path, bytes.NewReader(rawBody))
	if err != nil {
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
	fmt.Printf("Body: %s\n", body)
	responseMeta := protocol.Metadata{
		ID:         meta.ID,
		Type:       "response",
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
	}

	return protocol.WriteMessage(conn, responseMeta, bytes.NewReader(body), uint32(len(body)))
}
