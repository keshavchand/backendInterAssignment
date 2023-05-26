package queue

import (
	"sync"
	"time"
)

type WaitingStatus int

const (
	NoWaiting WaitingStatus = iota
	CurrentlyWaiting
)

type QueueNode struct {
	value string
	next  *QueueNode
}

type PrimitiveQueue struct {
	head   *QueueNode
	tail   *QueueNode
	status WaitingStatus
	cond   *sync.Cond
}

type OneToManyQueuePrimitive struct {
	lock  sync.Mutex
	Queue map[string]*PrimitiveQueue
}

var NodePool = sync.Pool{
	New: func() any {
		return &QueueNode{}
	},
}

func (q *OneToManyQueuePrimitive) QPush(key string, value []string) {
	if len(value) <= 0 {
		return
	}

	head := NodePool.Get().(*QueueNode)
	head.value = value[0]
	head.next = nil
	tail := head

	for _, v := range value[1:] {
		tail.next = &QueueNode{v, nil}
		tail = tail.next
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	queue, found := q.Queue[key]
	if !found {
		queue = new(PrimitiveQueue)
	}

	if queue.head == nil {
		queue.head = head
		queue.tail = tail
	} else {
		queue.tail.next = head
		queue.tail = tail
	}

	q.Queue[key] = queue

	if queue.status == CurrentlyWaiting {
		queue.cond.Signal()
	}
}

func (q *OneToManyQueuePrimitive) QPop(key string) (string, error) {
	return q.QPopTimeout(key, nil)
}

func (q *OneToManyQueuePrimitive) QPopTimeout(key string, time *time.Time) (string, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	queue, found := q.Queue[key]
	if !found {
		if time == nil {
			return "", ErrorEmptyQueue
		}
		queue = new(PrimitiveQueue)
		q.Queue[key] = queue
	}

	for queue.head == nil {
		if time == nil {
			return "", ErrorEmptyQueue
		}

		err := q.waitForValue(queue, *time)
		if err != nil {
			return "", err
		}

		time = nil
	}

	node := queue.head
	if queue.head == queue.tail {
		// Only Element
		queue.head = nil
		queue.tail = nil
	}
	if queue.head == nil {
		delete(q.Queue, key)
	} else {
		queue.head = queue.head.next
		q.Queue[key] = queue
	}

	NodePool.Put(node)
	return node.value, nil
}

// Errors if queue is already begin waited on
func (qP *OneToManyQueuePrimitive) waitForValue(q *PrimitiveQueue, timeout time.Time) error {
	if q.status == CurrentlyWaiting {
		return ErrorManyWaiterOnQueue
	}

	q.status = CurrentlyWaiting
	q.cond = sync.NewCond(&qP.lock)

	go func() {
		<-time.After(time.Until(timeout))
		q.cond.Signal()
	}()

	q.cond.Wait()
	q.status = NoWaiting
	return nil
}
