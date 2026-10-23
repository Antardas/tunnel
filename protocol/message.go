package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
)

const MaxMessageSize = 10 * 1024 * 1024
const MaxMetadataSize = 1 * 1024 * 1024
const MaxBodySize = 10 * 1024 * 1024

type Metadata struct {
	ID       string      `json:"id"`
	Type     string      `json:"type"`
	Method   string      `json:"method"`
	Path     string      `json:"path"`
	RawQuery string      `json:"rawQuery,omitempty"`
	StatusCode int `json:"statusCode,omitempty"`
	Headers  http.Header `json:"headers"`
}

/**
*
┌─────────────────────────┐
│ 4 bytes: metadata length│
├─────────────────────────┤
│ N bytes: JSON metadata  │
├─────────────────────────┤
│ 4 bytes: body length    │
├─────────────────────────┤
│ N bytes: raw body bytes │
└─────────────────────────┘
*/

func WriteMessage(conn net.Conn, meta Metadata, body io.Reader, bodyLength uint32) error {

	metadata, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if len(metadata) > MaxMetadataSize || bodyLength > MaxBodySize {
		return fmt.Errorf("messsage exceeds size limit")
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(metadata)))

	if err := writeAll(conn, length[:]); err != nil {
		return err
	}

	if err := writeAll(conn, metadata); err != nil {
		return err
	}

	binary.BigEndian.PutUint32(length[:], bodyLength)

	if err := writeAll(conn, length[:]); err != nil {
		return err
	}

	_, err = io.CopyN(conn, body, int64(bodyLength))

	return err
}

func ReadMessage(conn net.Conn) (Metadata, []byte, error) {
	var length [4]byte
	var meta Metadata

	if _, err := io.ReadFull(conn, length[:]); err != nil {
		return meta, nil, err
	}

	metaLen := binary.BigEndian.Uint32(length[:])

	if metaLen > MaxMetadataSize {
		return meta, nil, fmt.Errorf("metadata too large")
	}

	metadata := make([]byte, metaLen)
	if _, err := io.ReadFull(conn, metadata); err != nil {
		return meta, nil, err
	}

	if err := json.Unmarshal(metadata, &meta); err != nil {
		return meta, nil, err
	}

	if _, err := io.ReadFull(conn, length[:]); err != nil {
		return meta, nil, err
	}

	bodyLen := binary.BigEndian.Uint32(length[:])

	if bodyLen > MaxBodySize {
		return meta, nil, fmt.Errorf("body too large")
	}

	body := make([]byte, bodyLen)
	_, err := io.ReadFull(conn, body)
	
	return meta, body, err
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}

		data = data[n:]
	}

	return nil
}
