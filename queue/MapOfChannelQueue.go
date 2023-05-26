package queue

import (
	"sync"
	"sync/atomic"
	"time"
)

type MapOfChannelQueue struct {
	q      chan string
	locked atomic.Bool
}

func MOCQ() *MapOfChannelQueue {
	return &MapOfChannelQueue{
		q:      make(chan string, 100),
		locked: atomic.Bool{},
	}
}

type MapOfChannel struct {
	Queue map[string]*MapOfChannelQueue
	lock  sync.Mutex
}

func (q *MapOfChannel) QPush(key string, value []string) {
	q.lock.Lock()

	queue, found := q.Queue[key]
	if !found {
		queue = MOCQ()
		q.Queue[key] = queue
	}

	q.lock.Unlock()

	for _, v := range value {
		queue.q <- v
	}
}

func (q *MapOfChannel) QPop(key string) (string, error) {
	q.lock.Lock()
	queue, found := q.Queue[key]
	q.lock.Unlock()
	if !found {
		return "", ErrorEmptyQueue
	}

	if !queue.locked.CompareAndSwap(false, true) {
		return "", ErrorManyWaiterOnQueue
	}
	defer queue.locked.Store(false)

	select {
	case c := <-queue.q:
		return c, nil
	default:
		return "", ErrorEmptyQueue
	}
}

func (q *MapOfChannel) QPopTimeout(key string, t *time.Time) (string, error) {
	q.lock.Lock()
	queue, found := q.Queue[key]
	if !found {
		queue = MOCQ()
		q.Queue[key] = queue
	}
	q.lock.Unlock()

	if !queue.locked.CompareAndSwap(false, true) {
		return "", ErrorManyWaiterOnQueue
	}
	defer queue.locked.Store(false)

	if t == nil {
		select {
		case c := <-queue.q:
			return c, nil
		default:
			return "", ErrorEmptyQueue
		}
	}

	select {
	case <-time.After(time.Until(*t)):
		return "", ErrorEmptyQueue
	case c := <-queue.q:
		return c, nil
	}
}
