package server

import (
	"antardas/tcp-tunnel/protocol"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/google/uuid"
)

var (
	tunnelConn  = make(map[string]net.Conn)
	ClientHosts = map[string][]string{
		"1": {"port-5000.dev.oshudstock.com", "port-4000.dev.oshudstock.com"},
		"2": {"port-5147.dev.oshudstock.com", "port-7000.dev.oshudstock.com"},
	}
	HostClient = map[string]string{
		"port-5000.dev.oshudstock.com": "1",
		"port-4000.dev.oshudstock.com": "1",
		"port-5147.dev.oshudstock.com": "2",
		"port-7000.dev.oshudstock.com": "2",
	}

	HostPort = map[string]string{
		"port-5000.dev.oshudstock.com": "5000",
		"port-4000.dev.oshudstock.com": "4000",
		"port-5147.dev.oshudstock.com": "5147",
		"port-7000.dev.oshudstock.com": "7000",
	}
	tunnelMu sync.Mutex
)

func Start() {
	listener, err := net.Listen("tcp", ":5135")
	go startHTTPServer()

	if err != nil {
		panic(err)
	}

	defer listener.Close()

	fmt.Println("Server is listening 5135")

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Accept error", err)
			continue
		}
		meta, _, err := protocol.ReadMessage(conn)
		if err != nil {
			fmt.Println("Error reading message from client:")
			conn.Close()
			continue
		}
		if meta.ClientId == "" {
			fmt.Println("Rejected: ClientId is required")
			continue
		}
		tunnelMu.Lock()
		if meta.Type == "init" {
			tunnelConn[meta.ClientId] = conn
		}
		tunnelMu.Unlock()
		fmt.Println("Tunnel client connected: ", meta.ClientId)
	}

}

func startHTTPServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleHTTP)
	fmt.Println("HTTP server listen on :5136")

	if err := http.ListenAndServe(":5136", mux); err != nil {
		fmt.Println("HTTP server error: ", err)
	}
}

func handleHTTP(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	fmt.Println("Received: ", r.Method, r.URL.Path, r.URL.Port())

	hostname, _, err := net.SplitHostPort(host)

	if err == nil {
		host = hostname
	}

	clientId, ok := HostClient[host]
	if !ok {
		http.Error(w, "Unknowd host", http.StatusNotFound)
		return
	}

	hostPort, ok := HostPort[host]
	if !ok {
		http.Error(w, "Port not configured", http.StatusNotFound)
		return
	}

	fmt.Println("Host: ", host)
	fmt.Println("Client ID: ", clientId)
	fmt.Println("Local Port", hostPort)

	body, err := io.ReadAll(r.Body)

	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	meta := protocol.Metadata{
		ID:         uuid.NewString(),
		Type:       "request",
		ClientHost: host,
		ClientPort: hostPort,
		Method:     r.Method,
		Path:       r.URL.Path,
		RawQuery:   r.URL.RawQuery,
		Headers:    r.Header,
	}
	tunnelMu.Lock()
	tunnelConnClient, ok := tunnelConn[clientId]
	tunnelMu.Unlock()
	if !ok {
		http.Error(w, "Client not establieshed connection", http.StatusBadGateway)
		return
	}
	if err := protocol.WriteMessage(tunnelConnClient, meta, bytes.NewReader(body), uint32(len(body))); err != nil {
		_ = tunnelConnClient.Close()
		http.Error(w, "Failed to send request to tunnel", http.StatusBadGateway)
		return
	}

	responseMeta, responseBody, err := protocol.ReadMessage(tunnelConnClient)

	if err != nil {
		_ = tunnelConnClient.Close()
		http.Error(w, "Failed to receive tunnel response", http.StatusBadGateway)
		return
	}

	if meta.ID != responseMeta.ID {
		http.Error(w, "Tunnel response mismatch", http.StatusBadGateway)
		return
	}

	for key, values := range responseMeta.Headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	status := responseMeta.StatusCode

	if status == 0 {
		status = http.StatusBadGateway
	}

	w.WriteHeader(status)
	_, _ = w.Write(responseBody)

}
