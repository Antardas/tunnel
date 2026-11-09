package client

import (
	"antardas/tcp-tunnel/protocol"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ClientConnection struct {
	Conn    net.Conn
	WriteMu sync.Mutex
}

var activeClientId string
var localTransport = http.DefaultTransport.(*http.Transport).Clone()

var localHTTPClient = &http.Client{
	Transport: localTransport,
}

func Connect(clientId string) {
	// localTransport.MaxIdleConns = 100
	// localTransport.MaxIdleConnsPerHost = 100
	// localTransport.IdleConnTimeout = 90 * time.Second
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
	// conn, err := net.DialTimeout("tcp", "localhost:5135", 5*time.Second)

	if err != nil {
		return err
	}

	clientConn := &ClientConnection{
		Conn: conn,
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
			if errors.Is(err, io.EOF) {
				log.Println("Client disconnected")
				return nil
			}

			fmt.Printf("reading request failed: %v\n", err)

			continue
		}

		if meta.Type != "request" {
			fmt.Println("Unexpected message type:", meta.Type)
			continue
		}
		go func() {
			if err := callService(clientConn, meta, body); err != nil {
				fmt.Println("Reading Request failed:", err)

			}
		}()

	}
}

func callService(clinetConn *ClientConnection, meta protocol.Metadata, rawBody []byte) error {
	started := time.Now()
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
	requestStarted := time.Now()

	resp, err := localHTTPClient.Do(req)
	fmt.Printf(
		"[CLIENT] %s HTTP request: %v\n",
		meta.Path,
		time.Since(requestStarted),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bodyStarted := time.Now()

	body, err := io.ReadAll(resp.Body)
	fmt.Printf(
		"[CLIENT] %s Read body: %v\n",
		meta.Path,
		time.Since(bodyStarted),
	)
	if err != nil {
		responseMeta := protocol.Metadata{
			ID:         meta.ID,
			ClientId:   activeClientId,
			Type:       "response",
			StatusCode: http.StatusBadGateway,
			Headers:    make(http.Header),
		}
		body := []byte("Local service unavailable: " + err.Error())
		clinetConn.WriteMu.Lock()
		err = protocol.WriteMessage(clinetConn.Conn, responseMeta, bytes.NewReader(body), uint32(len(body)))
		clinetConn.WriteMu.Unlock()
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
	lockStarted := time.Now()

	clinetConn.WriteMu.Lock()
	fmt.Printf(
		"[CLIENT] %s Write lock wait: %v\n",
		meta.Path,
		time.Since(lockStarted),
	)
	writeStarted := time.Now()

	err = protocol.WriteMessage(clinetConn.Conn, responseMeta, bytes.NewReader(body), uint32(len(body)))
	fmt.Printf(
		"[CLIENT] %s TCP write: %v\n",
		meta.Path,
		time.Since(writeStarted),
	)
	clinetConn.WriteMu.Unlock()
	fmt.Printf(
		"[CLIENT] %s Total: %v\n",
		meta.Path,
		time.Since(started),
	)

	return err
}
