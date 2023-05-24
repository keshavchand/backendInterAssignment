package main

import (
	"errors"
	"log"
	"testing"
	"time"
)

func TestSetAndGet(t *testing.T) {
	storage := NewStorage()
	storage.Set("key", "value1", nil)
	err := storage.SetIfExists("key", "value2", nil)
	if err != nil {
		t.Fatal(err)
	}

	str, err := storage.Get("key")
	if err != nil {
		t.Fatal(err)
	}

	if str != "value2" {
		t.Fatalf("Expected value2, got %s", str)
	}

	err = storage.SetIfDoesntExists("key", "value3", nil)
	if !errors.Is(err, ErrorKeyExists) {
		t.Fatal(err)
	}
}

func TestSetWithExpiry(t *testing.T) {
	storage := NewStorage()
	expireTime := new(time.Time)
	*expireTime = time.Now().Add(1 * time.Second)

	storage.Set("key", "value1", expireTime)
	err := storage.SetIfExists("key", "value2", expireTime)
	if err != nil {
		t.Fatal(err)
	}

	_, err = storage.Get("key")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	_, err = storage.Get("key")
	if err != ErrorKeyNotFound {
		t.Fatal("Expected error, got nil")
	}

	err = storage.SetIfDoesntExists("key", "value3", nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueue(t *testing.T) {
	storage := NewStorage()

	storage.QPush("queue", []string{"value1"})
	str, err := storage.QPop("queue")
	if err != nil {
		log.Fatal(err)
	}

	str, err = storage.QPop("queue")
	if err != ErrorEmptyQueue {
		t.Fatalf("Expected error, got %+v", err)
	}

	go func() {
		time.Sleep(2 * time.Second)
		storage.QPush("queue", []string{"value2"})
	}()

	go func() {
		time.Sleep(1 * time.Second)
		expireTime := new(time.Time)
		*expireTime = time.Now().Add(1 * time.Second)

		_, err := storage.QPopTimeout("queue", expireTime)
		if err != ErrorManyWaiterOnQueue {
			t.Errorf("Expected error, got %+v", err)
		}
	}()

	expireTime := new(time.Time)
	*expireTime = time.Now().Add(100000 * time.Second)
	str, err = storage.QPopTimeout("queue", expireTime)
	if err != nil {
		t.Fatal(err)
	}
	if str != "value2" {
		t.Fatalf("Expected value2, got %s", str)
	}
}

func TestParallelReader(t *testing.T) {
	storage := NewStorage()

	expireTime := new(time.Time)
	*expireTime = time.Now().Add(2 * time.Second)

	go func() {
		time.Sleep(1 * time.Second)
		_, err := storage.QPopTimeout("queue", expireTime)
		if err != ErrorManyWaiterOnQueue {
			t.Errorf("Expected error, got %+v", err)
		}
	}()

	_, err := storage.QPopTimeout("queue", expireTime)
	if err != ErrorEmptyQueue {
		t.Errorf("Expected error, got %+v", err)
	}
}

func TestScenarios(t *testing.T) {
	type cmdFn func(*Storage)
	table := [][]cmdFn{
		[]cmdFn{
			func(s *Storage) { s.QPush("key", []string{"value1"}) },
			func(s *Storage) {
				str, _ := s.QPopTimeout("key", nil)
				if str != "value1" {
					t.Errorf("Test Failed")
				}
			},
		},

		[]cmdFn{
			func(s *Storage) {
				zeroExpire := new(time.Time)
				*zeroExpire = time.Now().Add(0 * time.Second)
				_, err := s.QPopTimeout("key", zeroExpire)
				if err != ErrorEmptyQueue {
					t.Errorf("Test Failed")
				}
			},
		},

		[]cmdFn{
			func(s *Storage) {
				start := time.Now()
				tenExpire := new(time.Time)
				*tenExpire = time.Now().Add(1 * time.Second)
				_, err := s.QPopTimeout("key", tenExpire)
				if err != ErrorEmptyQueue {
					t.Errorf("Test Failed")
				}

				if time.Since(start) < 1*time.Second {
					t.Error("Should wait for at least 10 seconds")
				}
			},
		},
	}

	for idx, t := range table {
		storage := NewStorage()
		for _, fn := range t {
			fn(storage)
		}
	}
}
