package reverseproxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// failingDialTransport simulates a dial timeout / failure.
type failingDialTransport struct{}

func (f *failingDialTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		// Go's http.Transport closes req.Body on dial failure
		_ = req.Body.Close()
	}
	return nil, &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: errors.New("i/o timeout"),
	}
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

	// Emulate downstream error handler (handle_errors)
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

	bodyBytes, readErr := io.ReadAll(req.Body)
	if readErr != nil {
		t.Fatalf("failed to read buffered req.Body in error handler: %v", readErr)
	}

	if string(bodyBytes) != payload {
		t.Fatalf("expected payload %q, got %q", payload, string(bodyBytes))
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
