package main

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

type Command interface{}

type Set struct {
	Command

	Key    string
	Value  string
	Expiry *time.Time

	XX bool // Set if key exists
	NX bool // Set if key doesn't exists
}

type Get struct {
	Command

	Key string
}

type QPush struct {
	Command

	Key   string
	Value []string
}

type QPop struct {
	Command

	Key string
}

type BQPop struct {
	Command

	Key     string
	Timeout *time.Time
}

func ParseCommand(command string) (Command, error) {
	parts := strings.Split(command, " ")
	if len(parts) < 2 {
		return nil, ErrorInvalidCommand
	}

	switch parts[0] {
	case "SET":
		return parseSetCommand(parts[1:])
	case "GET":
		return parseGetCommand(parts[1:])
	case "QPUSH":
		return parseQPushCommand(parts[1:])
	case "QPOP":
		return parseQPopCommand(parts[1:])
	case "BQPOP":
		return parseBQPopCommand(parts[1:])
	default:
		return nil, ErrorInvalidCommand
	}
}

var (
	ErrorInvalidCommand      = errors.New("invalid command")
	ErrorInvalidGetCommand   = errors.New("invalid get command")
	ErrorInvalidSetCommand   = errors.New("invalid set command")
	ErrorInvalidQPushCommand = errors.New("invalid qpush command")
	ErrorInvalidQPopCommand  = errors.New("invalid qpop command")
	ErrorInvalidBQPopCommand = errors.New("invalid bqpop command")
)

func parseQPushCommand(parts []string) (qpush QPush, nil error) {
	if len(parts) < 2 {
		return qpush, ErrorInvalidQPushCommand
	}

	qpush.Key = parts[0]
	qpush.Value = parts[1:]
	return
}

func parseGetCommand(parts []string) (get Get, nil error) {
	if len(parts) < 1 {
		return get, ErrorInvalidGetCommand
	}

	get.Key = parts[0]
	return
}

func parseQPopCommand(parts []string) (qpop QPop, nil error) {
	if len(parts) < 1 {
		return qpop, ErrorInvalidQPopCommand
	}

	qpop.Key = parts[0]
	return
}

func parseBQPopCommand(parts []string) (bqpop BQPop, nil error) {
	if len(parts) < 1 {
		return bqpop, ErrorInvalidBQPopCommand
	}

	bqpop.Key = parts[0]
	if len(parts) < 2 {
		return bqpop, nil
	}

	timeout, err := strconv.Atoi(parts[1])
	if err != nil || timeout < 0 {
		return bqpop, ErrorInvalidBQPopCommand
	}
	bqpop.Timeout = new(time.Time)
	*bqpop.Timeout = time.Now().Add(time.Duration(timeout) * time.Second)

	return
}

func parseSetCommand(parts []string) (set Set, nil error) {
	// Key value expiry? condition?
	if len(parts) < 2 {
		return set, ErrorInvalidSetCommand
	}

	set.Key = parts[0]
	set.Value = parts[1]

	// No expiry no condition
	if len(parts) < 3 {
		return
	}

	parts = parts[2:]
	for len(parts) > 0 {
		cmd := parts[0]

		switch cmd {
		case "EX":
			if len(parts) < 2 {
				return set, ErrorInvalidSetCommand
			}
			expiry, err := strconv.Atoi(parts[1])
			if err != nil || expiry < 0 {
				return set, ErrorInvalidSetCommand
			}

			set.Expiry = new(time.Time)
			*set.Expiry = time.Now().Add(time.Duration(expiry) * time.Second)
			parts = parts[2:]

		case "XX":
			set.XX = true
			parts = parts[1:]

		case "NX":
			set.NX = true
			parts = parts[1:]

		default:
			return set, ErrorInvalidSetCommand
		}
	}

	return set, nil
}
