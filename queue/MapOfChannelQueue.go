package queue

import (
	"sync"
	"time"
)

type MapOfChannel struct {
	Queue map[string]chan string
	lock  sync.Mutex
}

func (q *MapOfChannel) QPush(key string, value []string) {
	q.lock.Lock()

	queue, found := q.Queue[key]
	if !found {
		queue = make(chan string, 100)
		q.Queue[key] = queue
	}

	q.lock.Unlock()
	for _, v := range value {
		queue <- v
	}
}

func (q *MapOfChannel) QPop(key string) (string, error) {
	q.lock.Lock()
	queue, found := q.Queue[key]
	q.lock.Unlock()
	if !found {
		return "", ErrorKeyNotFound
	}

	select {
	case c := <-queue:
		return c, nil
	default:
		return "", ErrorEmptyQueue
	}
}

func (q *MapOfChannel) QPopTimeout(key string, t *time.Time) (string, error) {
	q.lock.Lock()
	queue, found := q.Queue[key]
	q.lock.Unlock()
	if !found {
		return "", ErrorKeyNotFound
	}

	select {
	case <-time.After(time.Until(*t)):
		return "", ErrorEmptyQueue
	case c := <-queue:
		return c, nil
	}
}
