package reverseproxy

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// failingDialTransport simulates a dial timeout / connection failure where transport closes body before read.
type failingDialTransport struct{}

func (f *failingDialTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		// Go's net/http Transport closes req.Body on dial failures
		_ = req.Body.Close()
	}
	return nil, &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: errors.New("i/o timeout"),
	}
}

// partiallyReadingFailingTransport reads partial bytes from request body and then errors.
type partiallyReadingFailingTransport struct {
	bytesToRead int
}

func (p *partiallyReadingFailingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		buf := make([]byte, p.bytesToRead)
		_, _ = req.Body.Read(buf)
		_ = req.Body.Close()
	}
	return nil, errors.New("connection reset by peer during payload transmission")
}

func TestReverseProxyDialTimeoutPreservesRequestBody(t *testing.T) {
	payload := "important_payload_data_to_preserve"
	handler := &Handler{
		Transport: &failingDialTransport{},
	}

	req := httptest.NewRequest(http.MethodPost, "http://localhost:8080/test", strings.NewReader(payload))
	rec := httptest.NewRecorder()

	err := handler.ServeHTTP(rec, req, nil)
	if err == nil {
		t.Fatal("expected error from dial timeout, got nil")
	}

	// Downstream error handler (handle_errors) reading body
	bodyBytes, readErr := io.ReadAll(req.Body)
	if readErr != nil {
		t.Fatalf("failed to read req.Body in error handler: %v", readErr)
	}

	if string(bodyBytes) != payload {
		t.Fatalf("expected payload %q, got %q", payload, string(bodyBytes))
	}
}

func TestReverseProxyBufferedRequestBodyPreservedOnDialFailure(t *testing.T) {
	payload := "buffered_payload_data"
	handler := &Handler{
		Transport:      &failingDialTransport{},
		BufferRequests: true,
	}

	req := httptest.NewRequest(http.MethodPut, "http://localhost:8080/test", strings.NewReader(payload))
	rec := httptest.NewRecorder()

	err := handler.ServeHTTP(rec, req, nil)
	if err == nil {
		t.Fatal("expected error from dial timeout, got nil")
	}

	// Downstream error handler (handle_errors) reading buffered body
	bodyBytes, readErr := io.ReadAll(req.Body)
	if readErr != nil {
		t.Fatalf("failed to read buffered req.Body in error handler: %v", readErr)
	}

	if string(bodyBytes) != payload {
		t.Fatalf("expected payload %q, got %q", payload, string(bodyBytes))
	}
}

func TestReverseProxyPartiallyReadStreamingBodySafeFallback(t *testing.T) {
	payload := "streaming_data_that_fails_midway"
	handler := &Handler{
		Transport: &partiallyReadingFailingTransport{bytesToRead: 10},
	}

	req := httptest.NewRequest(http.MethodPost, "http://localhost:8080/test", strings.NewReader(payload))
	rec := httptest.NewRecorder()

	err := handler.ServeHTTP(rec, req, nil)
	if err == nil {
		t.Fatal("expected error from transport, got nil")
	}

	// Error handlers or logging middleware attempting to read body must get EOF / empty reader, not panic
	bodyBytes, readErr := io.ReadAll(req.Body)
	if readErr != nil {
		t.Fatalf("unexpected error reading fallback body: %v", readErr)
	}
	if len(bodyBytes) != 0 {
		t.Fatalf("expected empty fallback body for consumed stream, got %d bytes", len(bodyBytes))
	}
}

func TestReverseProxyNoBodySafeOnDialFailure(t *testing.T) {
	handler := &Handler{
		Transport: &failingDialTransport{},
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/test", nil)
	rec := httptest.NewRecorder()

	err := handler.ServeHTTP(rec, req, nil)
	if err == nil {
		t.Fatal("expected error from dial timeout, got nil")
	}

	if req.Body != nil && req.Body != http.NoBody {
		bodyBytes, readErr := io.ReadAll(req.Body)
		if readErr != nil {
			t.Fatalf("unexpected error reading empty req.Body: %v", readErr)
		}
		if len(bodyBytes) != 0 {
			t.Fatalf("expected 0 bytes, got %d", len(bodyBytes))
		}
	}
}

func TestReverseProxySuccessStreamingBody(t *testing.T) {
	payload := "streamed_payload"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer server.Close()

	handler := &Handler{
		Transport: http.DefaultTransport,
	}

	req := httptest.NewRequest(http.MethodPost, server.URL, strings.NewReader(payload))
	rec := httptest.NewRecorder()

	err := handler.ServeHTTP(rec, req, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if rec.Body.String() != payload {
		t.Fatalf("expected response body %q, got %q", payload, rec.Body.String())
	}
}

func TestDialProtectedBodyLifecycle(t *testing.T) {
	// Case 1: Close without read does not close underlying reader
	r1 := io.NopCloser(strings.NewReader("hello"))
	p1 := newDialProtectedBody(r1)
	if p1.HasReadStarted() {
		t.Fatal("expected readStarted to be false")
	}
	if err := p1.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	buf := make([]byte, 5)
	n, err := p1.Read(buf)
	if err != nil || n != 5 || string(buf) != "hello" {
		t.Fatalf("expected reading after unread Close to succeed, got n=%d, err=%v", n, err)
	}

	// Case 2: Read then Close closes underlying reader
	r2 := io.NopCloser(bytes.NewBufferString("world"))
	p2 := newDialProtectedBody(r2)
	buf2 := make([]byte, 5)
	_, _ = p2.Read(buf2)
	if !p2.HasReadStarted() {
		t.Fatal("expected readStarted to be true")
	}
	if err := p2.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
