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
