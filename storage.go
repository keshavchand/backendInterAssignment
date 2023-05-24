package main

import (
	"errors"
	"sync"
	"time"
)

type WaitingStatus int

const (
	NoWaiting WaitingStatus = iota
	CurrentlyWaiting
)

// Nodes are implemented in a linked list fashion
type QueueNode struct {
	value string
	next  *QueueNode
}

type Queue struct {
	tail   *QueueNode
	status WaitingStatus
}

type Value struct {
	value  string
	expiry *time.Time
}

type Storage struct {
	KV     map[string]Value
	kvLock sync.RWMutex

	Queue     map[string]*Queue
	queueLock sync.Mutex
}

func NewStorage() *Storage {
	return &Storage{
		KV:    make(map[string]Value),
		Queue: make(map[string]*Queue),
	}
}

func (v *Value) Expired() bool {
	if v.expiry == nil {
		return false
	}

	return time.Now().After(*v.expiry)
}

var (
	ErrorKeyNotFound       = errors.New("key not found")
	ErrorKeyPresent        = errors.New("key already exists")
	ErrorEmptyQueue        = errors.New("queue is empty")
	ErrorManyWaiterOnQueue = errors.New("many waiter on queue")
)

func (s *Storage) SetIfExists(key, value string, expiry *time.Time) error {
	s.kvLock.Lock()
	defer s.kvLock.Unlock()

	if vOld, ok := s.KV[key]; ok && !vOld.Expired() {
		s.KV[key] = Value{value, expiry}
		return nil
	}

	return ErrorKeyNotFound
}

func (s *Storage) SetIfDoesntExists(key, value string, expiry *time.Time) error {
	s.kvLock.Lock()
	defer s.kvLock.Unlock()

	if vOld, ok := s.KV[key]; !ok || !vOld.Expired() {
		return ErrorKeyPresent
	}

	s.KV[key] = Value{value, expiry}
	return nil
}

func (s *Storage) Set(key, value string, expiry *time.Time) {
	s.kvLock.Lock()
	defer s.kvLock.Unlock()
	s.KV[key] = Value{value, expiry}
}

func (s *Storage) Get(key string) (string, error) {
	s.kvLock.RLock()
	defer s.kvLock.RUnlock()

	v, ok := s.KV[key]
	if !ok || v.Expired() {
		return "", ErrorKeyNotFound
	}

	return v.value, nil
}

func (s *Storage) QPush(key string, value []string) {
	if len(value) <= 0 {
		return
	}

	head := &QueueNode{value[0], nil}
	tail := head

	for _, v := range value[1:] {
		tail = &QueueNode{v, tail}
		tail = tail.next
	}

	s.queueLock.Lock()
	defer s.queueLock.Unlock()

	queue := s.Queue[key]
	head.next = queue.tail
	queue.tail = tail
	s.Queue[key] = queue
}

func (s *Storage) QPop(key string) (string, error) {
	return s.QPopTimeout(key, nil)
}

func (s *Storage) QPopTimeout(key string, time *time.Time) (string, error) {
	s.queueLock.Lock()
	defer s.queueLock.Unlock()

	queue, found := s.Queue[key]
	if !found {
		if time == nil {
			return "", ErrorEmptyQueue
		}

		q := new(Queue)
		s.Queue[key] = q
		err := s.waitForValue(q, *time)
		if err != nil {
			return "", err
		}
	}

	node := queue.tail
	if node == nil {
		return "", nil
	}

	queue.tail = queue.tail.next
	if queue.tail == nil {
		delete(s.Queue, key)
	} else {
		s.Queue[key] = queue
	}

	return node.value, nil
}

// Errors if queue is already begin waited on
func (s *Storage) waitForValue(q *Queue, timeout time.Time) error {
	if q.status == CurrentlyWaiting {
		return ErrorManyWaiterOnQueue
	}

	q.status = CurrentlyWaiting
	cond := sync.NewCond(&s.queueLock)

	go func() {
		<-time.After(time.Until(timeout))
		cond.Signal()
	}()

	cond.Wait()
	q.status = NoWaiting
	return nil
}
