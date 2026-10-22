package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

const MaxMessageSize = 10 * 1024 * 1024

func WriteMessage(w io.Writer, data []byte) error {
	if len(data) > MaxMessageSize {
		return fmt.Errorf("message too large %d bytes", len(data))
	}

	var header [4]byte

	binary.BigEndian.PutUint32(header[:], uint32(len(data)))

	if err := writeAll(w, header[:]); err != nil {
		return err
	}

	return writeAll(w, data[:])
}

func ReadMessage(r io.Reader) ([]byte, error) {
	var header [4]byte

	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(header[:])

	if size > MaxMessageSize {
		return nil, fmt.Errorf("message  too large: %d bytes", size)
	}

	data := make([]byte, int(size))
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return data, nil
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
