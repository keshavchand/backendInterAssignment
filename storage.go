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
	cond   *sync.Cond
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
	s := &Storage{
		KV:    make(map[string]Value),
		Queue: make(map[string]*Queue),
	}

	go s.runStorageGC()
	return s
}

func (v *Value) Expired() bool {
	if v.expiry == nil {
		return false
	}

	return time.Now().After(*v.expiry)
}

var (
	ErrorKeyNotFound       = errors.New("key not found")
	ErrorKeyExists         = errors.New("key already exists")
	ErrorEmptyQueue        = errors.New("queue is empty")
	ErrorManyWaiterOnQueue = errors.New("many waiter on queue")
)

func (s *Storage) runStorageGC() {
	ticker := time.NewTicker(10 * time.Second)

	for {
		<-ticker.C
		s.kvLock.Lock()
		for k, v := range s.KV {
			if v.Expired() {
				delete(s.KV, k)
			}
		}
		s.kvLock.Unlock()
	}
}

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
		return ErrorKeyExists
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
	}

	s.queueLock.Lock()
	defer s.queueLock.Unlock()

	queue, found := s.Queue[key]
	if !found {
		queue = new(Queue)
	}
	head.next = queue.tail
	queue.tail = tail
	s.Queue[key] = queue

	if queue.status == CurrentlyWaiting {
		queue.cond.Signal()
	}
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
		queue = new(Queue)
		s.Queue[key] = queue
	}

	for queue.tail == nil {
		if time == nil {
			return "", ErrorEmptyQueue
		}

		err := s.waitForValue(queue, *time)
		if err != nil {
			return "", err
		}

		time = nil
	}

	node := queue.tail
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
	q.cond = sync.NewCond(&s.queueLock)

	go func() {
		<-time.After(time.Until(timeout))
		q.cond.Signal()
	}()

	q.cond.Wait()
	q.status = NoWaiting
	return nil
}
