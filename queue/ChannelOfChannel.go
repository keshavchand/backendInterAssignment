package queue

import (
	"sync"
	"sync/atomic"
	"time"
)

type QueueRequestInfo interface{}
type QueueSet struct {
	QueueRequestInfo
	Key   string
	Value []string
}

type Resp struct {
	Value string
	Error error
}

type QueueGet struct {
	QueueRequestInfo
	Key  string
	Resp chan Resp
}

type QueueGetTimeout struct {
	QueueRequestInfo
	Key  string
	Resp chan Resp
	Time time.Time
}

type ChannelofChannels struct {
	RequestQueue chan QueueRequestInfo
}

var ChannelPool = sync.Pool{
	New: func() any {
		return make(chan Resp, 0)
	},
}

type QI struct {
	c chan string
	r *atomic.Bool // true iff already begin waited on by a reader
}

func newQI() *QI {
	return &QI{
		c: make(chan string, 100),
	}
}

func (q *ChannelofChannels) Manager() {
	qinfo := make(map[string]*QI)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		info, ok := <-q.RequestQueue
		if !ok {
			return
		}

		switch qs := info.(type) {
		case QueueSet:
			queue, found := qinfo[qs.Key]
			if !found {
				queue = newQI()
				qinfo[qs.Key] = queue
			}

			for _, v := range qs.Value {
				queue.c <- v
			}

		case QueueGet:
			queue, found := qinfo[qs.Key]
			if !found {
				qs.Resp <- Resp{Error: ErrorKeyNotFound}
				continue
			}

			if len(queue.c) == 0 {
				qs.Resp <- Resp{Error: ErrorEmptyQueue}
				continue
			}

			// Note: No need to check the len of Resp channel
			// It is provided by the client from the channel pool
			// Either it is newly created or put in the pool
			c := <-queue.c
			qs.Resp <- Resp{Value: c}

		case QueueGetTimeout:
			queue, found := qinfo[qs.Key]
			if !found {
				qs.Resp <- Resp{Error: ErrorKeyNotFound}
				continue
			}

			if queue.r.Load() {
				qs.Resp <- Resp{Error: ErrorManyWaiterOnQueue}
				continue
			}
			queue.r.Store(true)

			wg.Add(1)
			go func(queue *QI) {
				defer wg.Done()

				select {
				case c := <-queue.c:
					qs.Resp <- Resp{Value: c}
				case <-time.After(time.Until(qs.Time)):
					qs.Resp <- Resp{Error: ErrorEmptyQueue}
				}

				queue.r.Store(false)
			}(queue)
		}
	}
}

func (q *ChannelofChannels) QPush(key string, value []string) {
	q.RequestQueue <- QueueSet{Key: key, Value: value}
}

func (q *ChannelofChannels) QPop(key string) (string, error) {
	resp := ChannelPool.Get().(chan Resp)
	q.RequestQueue <- QueueGet{Key: key, Resp: resp}
	r := <-resp
	ChannelPool.Put(resp)
	return r.Value, r.Error
}

func (q *ChannelofChannels) QPopTimeout(key string, time *time.Time) (string, error) {
	resp := ChannelPool.Get().(chan Resp)
	q.RequestQueue <- QueueGetTimeout{Key: key, Resp: resp, Time: *time}
	r := <-resp
	ChannelPool.Put(resp)
	return r.Value, r.Error
}
