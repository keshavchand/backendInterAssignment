package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestHttp(t *testing.T) {

	var req struct {
		Command string `json:"command"`
	}

	var resp struct {
		Value string `json:"value,omitempty"`
		Error string `json:"error,omitempty"`
	}

	processJson := func() {
		resp.Value = ""
		resp.Error = ""

		var buffer bytes.Buffer
		json.NewEncoder(&buffer).Encode(&req)

		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/", &buffer)

		HandleCommand(w, r)
		json.Unmarshal(w.Body.Bytes(), &resp)
	}

	storage = NewStorage()
	req.Command = "SET hello world"
	processJson()
	if resp.Error != "" || resp.Value != "" {
		t.Fatalf("Expected empty response got %+v", resp)
	}

	req.Command = "123 SET hello world"
	processJson()
	if resp.Error != "invalid command" {
		t.Fatalf("Expected error got %+v", resp)
	}

	req.Command = "GET hello"
	processJson()
	if resp.Value != "world" {
		t.Fatalf("Expected world got %+v", resp)
	}

	req.Command = "GET hello-123"
	processJson()
	if resp.Error != "key not found" {
		t.Fatalf("Expected key not found error got %+v", resp)
	}

	req.Command = "QPUSH list_a a"
	processJson()
	if resp.Error != "" || resp.Value != "" {
		t.Fatalf("Expected empty response got %+v", resp)
	}

	req.Command = "QPOP list_a"
	processJson()
	if resp.Error != "" || resp.Value != "a" {
		t.Fatalf("Expected just `a` in response got %+v", resp)
	}

	req.Command = "QPOP list_a"
	processJson()
	if resp.Error != "queue is empty" {
		t.Fatalf("Expected error in response got %+v", resp)
	}
}
