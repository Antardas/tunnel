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
	tunnelConn net.Conn
	tunnelMu   sync.Mutex
)

func Start() {
	listener, err := net.Listen("tcp", ":8080")
	go startHTTPServer()

	if err != nil {
		panic(err)
	}

	defer listener.Close()

	fmt.Println("Server is listening 8080")

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Accept error", err)
			continue
		}
		tunnelMu.Lock()
		if tunnelConn != nil {
			_ = tunnelConn.Close()
		}

		tunnelConn = conn
		tunnelMu.Unlock()
		fmt.Println("Tunnel client connected")
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
	fmt.Println("Received: ", r.Method, r.URL.Path)

	tunnelMu.Lock()
	defer tunnelMu.Unlock()

	if tunnelConn == nil {
		http.Error(w, "Tunnel client not connected", http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(r.Body)

	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	meta := protocol.Metadata{
		ID:       uuid.NewString(),
		Type:     "request",
		Method:   r.Method,
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
		Headers:  r.Header,
	}

	if err := protocol.WriteMessage(tunnelConn, meta, bytes.NewReader(body), uint32(len(body))); err != nil {
		_ = tunnelConn.Close()
		tunnelConn = nil
		http.Error(w, "Failed to send request to tunnel", http.StatusBadGateway)
		return
	}

	responseMeta, responseBody, err := protocol.ReadMessage(tunnelConn)

	if err != nil {
		_ = tunnelConn.Close()
		tunnelConn = nil
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
