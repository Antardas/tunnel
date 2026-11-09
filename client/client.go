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
	if err != nil {
		return err
	}

	clientConn := &ClientConnection{Conn: conn}
	defer conn.Close()

	meta := protocol.Metadata{
		ID:       uuid.NewString(),
		ClientId: clientId,
		Type:     "init",
	}
	if err := protocol.WriteMessage(conn, meta, bytes.NewReader(nil), 0); err != nil {
		return fmt.Errorf("sending init message: %w", err)
	}

	fmt.Println("Connected to the server")
	activeClientId = clientId

	for {
		readStarted := time.Now()
		meta, body, err := protocol.ReadMessage(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Println("Client disconnected")
				return nil
			}
			return fmt.Errorf("reading request from server after %v: %w", time.Since(readStarted), err)
		}

		if meta.Type != "request" {
			log.Printf("[CLIENT] unexpected message type=%s id=%s", meta.Type, meta.ID)
			continue
		}

		log.Printf("[CLIENT %s] TCP request received read_duration=%v body_bytes=%d method=%s path=%s", meta.ID, time.Since(readStarted), len(body), meta.Method, meta.Path)

		go func(requestMeta protocol.Metadata, requestBody []byte) {
			if err := callService(clientConn, requestMeta, requestBody); err != nil {
				log.Printf("[CLIENT %s] request processing error=%v", requestMeta.ID, err)
			}
		}(meta, body)
	}
}

func callService(clinetConn *ClientConnection, meta protocol.Metadata, rawBody []byte) error {
	started := time.Now()

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

	req, err := http.NewRequest(meta.Method, path, bytes.NewReader(rawBody))
	if err != nil {
		return err
	}
	req.Header = meta.Headers.Clone()

	localRequestStarted := time.Now()
	resp, err := localHTTPClient.Do(req)
	log.Printf("[CLIENT %s] local HTTP round_trip=%v", meta.ID, time.Since(localRequestStarted))
	if err != nil {
		return fmt.Errorf("local HTTP request: %w", err)
	}
	defer resp.Body.Close()

	bodyStarted := time.Now()
	body, err := io.ReadAll(resp.Body)
	log.Printf("[CLIENT %s] local response body read=%v bytes=%d", meta.ID, time.Since(bodyStarted), len(body))
	if err != nil {
		return fmt.Errorf("reading local response body: %w", err)
	}

	responseMeta := protocol.Metadata{
		ID:         meta.ID,
		ClientId:   activeClientId,
		Type:       "response",
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
	}

	lockStarted := time.Now()
	clinetConn.WriteMu.Lock()
	log.Printf("[CLIENT %s] TCP response write lock wait=%v", meta.ID, time.Since(lockStarted))

	writeStarted := time.Now()
	err = protocol.WriteMessage(clinetConn.Conn, responseMeta, bytes.NewReader(body), uint32(len(body)))
	log.Printf("[CLIENT %s] TCP response write=%v bytes=%d err=%v", meta.ID, time.Since(writeStarted), len(body), err)
	clinetConn.WriteMu.Unlock()

	log.Printf("[CLIENT %s] total processing=%v status=%d", meta.ID, time.Since(started), resp.StatusCode)
	return err
}
