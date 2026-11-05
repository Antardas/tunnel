package server

import (
	"antardas/tcp-tunnel/protocol"
	"net"
	"sync"
)

type PendingRequest struct {
	Response chan Response
}

type Response struct {
	Meta protocol.Metadata
	Body []byte
	Err  error
}

type TunnelConnection struct {
	Conn      net.Conn
	WriteMu   sync.Mutex
	PendingMu sync.Mutex
	Pending   map[string]chan Response
	Closed    bool
}

func (tc *TunnelConnection) ReadLoop(clientID string) {
	for {

		meta, body, err := protocol.ReadMessage(tc.Conn)

		if err != nil {
			tc.FailAll(err)
			tc.RemoveFromRegistry(clientID)
			return
		}

		tc.PendingMu.Lock()
		ch := tc.Pending[meta.ID]
		delete(tc.Pending, meta.ID)
		tc.PendingMu.Unlock()

		if ch != nil {
			ch <- Response{
				Meta: meta,
				Body: body,
			}
		}

	}
}

func (tc *TunnelConnection) FailAll(err error) {
	tc.PendingMu.Lock()
	defer tc.PendingMu.Unlock()
	if tc.Closed {
		return
	}

	tc.Closed = true
	tc.Conn.Close()

	for id, ch := range tc.Pending {
		ch <- Response{
			Err: err,
		}
		delete(tc.Pending, id)
	}
}

func (tc *TunnelConnection) RemoveFromRegistry(clientID string) {
	tunnelMu.Lock()
	defer tunnelMu.Unlock()

	if tunnelConn[clientID] == tc {
		delete(tunnelConn, clientID)
	}
}
