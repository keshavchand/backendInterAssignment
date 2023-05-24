package main

import (
	"strings"
	"testing"
	"time"
)

func TestSet(t *testing.T) {

	expireIn10Sec := new(time.Time)
	*expireIn10Sec = time.Now().Add(10 * time.Second)

	testCases := []struct {
		input  string
		output Set
	}{
		{"a 10", Set{Key: "a", Value: "10"}},
		{"a 10 EX 10",
			Set{
				Key:    "a",
				Value:  "10",
				Expiry: expireIn10Sec,
			},
		},
		{"a 10 NX",
			Set{
				Key:   "a",
				Value: "10",
				NX:    true,
			},
		},
	}

	for idx, tc := range testCases {
		input := strings.Split(tc.input, " ")
		set, err := parseSetCommand(input)
		if err != nil {
			t.Errorf("%d: %s %+v", idx, tc.input, err)
		}

		if set.Key != tc.output.Key {
			t.Errorf("%d: Expected %s, got %s", idx, tc.output.Key, set.Key)
		}
		if set.Value != tc.output.Value {
			t.Errorf("%d: Expected %s, got %s", idx, tc.output.Value, set.Value)
		}
		if tc.output.Expiry != nil && set.Expiry.Sub(*tc.output.Expiry) > 100*time.Millisecond {
			t.Errorf("Output Time different from expected")
		}
	}
}

func TestGet(t *testing.T) {
	testCases := []struct {
		input  string
		output Get
	}{
		{"a", Get{Key: "a"}},
	}

	for idx, tc := range testCases {
		input := strings.Split(tc.input, " ")
		get, err := parseGetCommand(input)
		if err != nil {
			t.Errorf("%d: %s %+v", idx, tc.input, err)
		}

		if get.Key != tc.output.Key {
			t.Errorf("%d: Expected %s, got %s", idx, tc.output.Key, get.Key)
		}
	}
}

func TestQPush(t *testing.T) {
	testCases := []struct {
		input  string
		output QPush
	}{
		{"a b c d e f", QPush{Key: "a", Value: []string{"b", "c", "d", "e", "f"}}},
		{"a b", QPush{Key: "a", Value: []string{"b"}}},
	}

	for idx, tc := range testCases {
		input := strings.Split(tc.input, " ")
		qPush, err := parseQPushCommand(input)
		if err != nil {
			t.Errorf("%d: %s %+v", idx, tc.input, err)
		}

		if qPush.Key != tc.output.Key {
			t.Errorf("%d: Expected %s, got %s", idx, tc.output.Key, qPush.Key)
		}

		for i, v := range qPush.Value {
			if v != tc.output.Value[i] {
				t.Errorf("%d: Expected %s, got %s", idx, tc.output.Value[i], v)
			}
		}
	}
}

func TestQPop(t *testing.T) {
	testCases := []struct {
		input  string
		output QPop
	}{
		{"a", QPop{Key: "a"}},
	}

	for idx, tc := range testCases {
		input := strings.Split(tc.input, " ")
		qPop, err := parseQPopCommand(input)
		if err != nil {
			t.Errorf("%d: %s %+v", idx, tc.input, err)
		}

		if qPop.Key != tc.output.Key {
			t.Errorf("%d: Expected %s, got %s", idx, tc.output.Key, qPop.Key)
		}
	}
}

func TestBQPop(t *testing.T) {
	expireIn10Sec := new(time.Time)
	*expireIn10Sec = time.Now().Add(10 * time.Second)

	testCases := []struct {
		input  string
		output BQPop
	}{
		{"a 10", BQPop{Key: "a", Timeout: expireIn10Sec}},
	}

	for idx, tc := range testCases {
		input := strings.Split(tc.input, " ")
		bqPop, err := parseBQPopCommand(input)
		if err != nil {
			t.Errorf("%d: %s %+v", idx, tc.input, err)
		}

		if bqPop.Key != tc.output.Key {
			t.Errorf("%d: Expected %s, got %s", idx, tc.output.Key, bqPop.Key)
		}

		if bqPop.Timeout.Sub(*tc.output.Timeout) > 100*time.Millisecond {
			t.Errorf("Output Time different from expected")
		}
	}
}
