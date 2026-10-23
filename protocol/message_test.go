package protocol

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	sender, receiver := net.Pipe()
	defer sender.Close()
	defer receiver.Close()

	wantMeta := Metadata{
		ID:       "req-123",
		Type:     "request",
		Method:   "POST",
		Path:     "/products/123",
		RawQuery: "color=blue&size=large",
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
			"Cookie":       []string{"session=abc"},
		},
	}
	// Include zero and non-UTF-8 bytes to verify that bodies remain raw bytes.
	wantBody := []byte{0x00, 0x7b, 0xff, 0x80, 0x7d}

	writeErr := make(chan error, 1)
	go func() {
		writeErr <- WriteMessage(sender, wantMeta, bytes.NewReader(wantBody), uint32(len(wantBody)))
	}()

	gotMeta, gotBody, readErr := ReadMessage(receiver)
	if readErr != nil {
		t.Fatalf("ReadMessage() error = %v", readErr)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("WriteMessage() error = %v", err)
	}
	fmt.Printf("Meta: %+v\n", gotMeta)

	if gotMeta.ID != wantMeta.ID ||
		gotMeta.Type != wantMeta.Type ||
		gotMeta.Method != wantMeta.Method ||
		gotMeta.Path != wantMeta.Path ||
		gotMeta.RawQuery != wantMeta.RawQuery {
		t.Fatalf("metadata mismatch: got %+v, want %+v", gotMeta, wantMeta)
	}
	if !reflect.DeepEqual(
		gotMeta.Headers.Values("Content-Type"),
		wantMeta.Headers.Values("Content-Type"),
	) {
		t.Fatalf("Content-Type mismatch: got %v, want %v", gotMeta.Headers, wantMeta.Headers)
	}
	if !bytes.Equal(gotBody, wantBody) {
		t.Fatalf("body mismatch: got %v, want %v", gotBody, wantBody)
	}
}
