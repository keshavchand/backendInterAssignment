package queue

import (
	"errors"
	"time"
)

type Queue interface {
	QPush(string, []string)
	QPop(string) (string, error)
	QPopTimeout(string, *time.Time) (string, error)
}

var (
	ErrorKeyNotFound       = errors.New("key not found")
	ErrorKeyExists         = errors.New("key already exists")
	ErrorEmptyQueue        = errors.New("queue is empty")
	ErrorManyWaiterOnQueue = errors.New("many waiter on queue")
)

const (
	QueueTypePrimitive = iota
	QueueTypeMapOfChannel
	QueueTypeChannel
)

func QueueFactory(t int) Queue {
	switch t {
	case QueueTypePrimitive:
		q := new(OneToManyQueuePrimitive)
		q.Queue = make(map[string]*PrimitiveQueue, 0)
		return q

	case QueueTypeMapOfChannel:
		q := new(MapOfChannel)
		q.Queue = make(map[string]*MapOfChannelQueue, 0)
		return q

	case QueueTypeChannel:
		q := new(ChannelofChannels)
		q.RequestQueue = make(chan QueueRequestInfo, 100)
		go q.Manager()
		return q
	default:
		panic("NOT IMPLEMENTED")
	}
}
