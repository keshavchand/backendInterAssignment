package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var storage *Storage

func main() {
	storage = NewStorage()

	mux := http.NewServeMux()
	mux.HandleFunc("/", HandleCommand)
	server := http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	server.ListenAndServe()
}

func HandleCommand(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Command string `json:"command"`
	}

	reader := io.LimitReader(r.Body, 1024)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		fmt.Fprintf(w, err.Error())
		return
	}

	err = json.Unmarshal([]byte(data), &req)
	if err != nil {
		jsonParsingError(w, r, err)
		return
	}

	var resp struct {
		Value string `json:"value,omitempty"`
		Error string `json:"error,omitempty"`
	}

	command, err := ParseCommand(req.Command)
	if err != nil {
		resp.Error = err.Error()
		sendResponseJson(w, http.StatusBadRequest, resp)
		return
	}

	value, err := processCommand(command)
	if err != nil {
		resp.Error = err.Error()
		sendResponseJson(w, http.StatusBadRequest, resp)
		return
	}

	resp.Value = value
	sendResponseJson(w, http.StatusOK, resp)
}

func sendResponseJson(w http.ResponseWriter, status int, r any) {
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(r)
}

// NOTE: the below code is from:
// https://www.alexedwards.net/blog/how-to-properly-parse-a-json-request-body
func jsonParsingError(w http.ResponseWriter, r *http.Request, err error) {
	var syntaxError *json.SyntaxError
	var unmarshalTypeError *json.UnmarshalTypeError

	switch {
	case errors.As(err, &syntaxError):
		msg := fmt.Sprintf("Request body contains badly-formed JSON (at position %d)", syntaxError.Offset)
		http.Error(w, msg, http.StatusBadRequest)

	case errors.Is(err, io.ErrUnexpectedEOF):
		msg := fmt.Sprintf("Request body contains badly-formed JSON")
		http.Error(w, msg, http.StatusBadRequest)

	case errors.As(err, &unmarshalTypeError):
		msg := fmt.Sprintf("Request body contains an invalid value for the %q field (at position %d)", unmarshalTypeError.Field, unmarshalTypeError.Offset)
		http.Error(w, msg, http.StatusBadRequest)

	case strings.HasPrefix(err.Error(), "json: unknown field "):
		fieldName := strings.TrimPrefix(err.Error(), "json: unknown field ")
		msg := fmt.Sprintf("Request body contains unknown field %s", fieldName)
		http.Error(w, msg, http.StatusBadRequest)

	case errors.Is(err, io.EOF):
		msg := "Request body must not be empty"
		http.Error(w, msg, http.StatusBadRequest)

	default:
		log.Print(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

func processCommand(c Command) (str string, err error) {
	switch c := c.(type) {
	case Set:
		if c.XX {
			err = storage.SetIfExists(c.Key, c.Value, c.Expiry)
		} else if c.NX {
			err = storage.SetIfDoesntExists(c.Key, c.Value, c.Expiry)
		} else {
			storage.Set(c.Key, c.Value, c.Expiry)
		}
		return

	case Get:
		return storage.Get(c.Key)

	case QPush:
		storage.QPush(c.Key, c.Value)
		return

	case QPop:
		return storage.QPop(c.Key)

	case BQPop:
		return storage.QPopTimeout(c.Key, c.Timeout)
	}

	return
}
