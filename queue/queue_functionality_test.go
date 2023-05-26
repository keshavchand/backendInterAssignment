package queue

import (
	"log"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	t.Run("QueueTypePrimitive", func(t *testing.T) {
		_TestQueue(t, QueueTypePrimitive)
	})
	t.Run("QueueTypeMapOfChannel", func(t *testing.T) {
		_TestQueue(t, QueueTypeMapOfChannel)
	})
	t.Run("QueueTypeChannel", func(t *testing.T) {
		_TestQueue(t, QueueTypeChannel)
	})
}

func _TestQueue(t *testing.T, queueType int) {
	queue := QueueFactory(queueType)

	queue.QPush("queue", []string{"value1"})
	str, err := queue.QPop("queue")
	if err != nil {
		log.Fatal(err)
	}

	str, err = queue.QPop("queue")
	if err != ErrorEmptyQueue {
		t.Fatalf("Expected error, got %+v", err)
	}

	go func() {
		time.Sleep(2 * time.Second)
		queue.QPush("queue", []string{"value2"})
	}()

	go func() {
		time.Sleep(1 * time.Second)
		expireTime := new(time.Time)
		*expireTime = time.Now().Add(1 * time.Second)

		_, err := queue.QPopTimeout("queue", expireTime)
		if err != ErrorManyWaiterOnQueue {
			t.Errorf("Expected error, got %+v", err)
		}
	}()

	expireTime := new(time.Time)
	*expireTime = time.Now().Add(100000 * time.Second)
	str, err = queue.QPopTimeout("queue", expireTime)
	if err != nil {
		t.Fatal(err)
	}
	if str != "value2" {
		t.Fatalf("Expected value2, got %s", str)
	}
}

func TestParallelReader(t *testing.T) {
	t.Run("QueueTypePrimitive", func(t *testing.T) {
		_TestParallelReader(t, QueueTypePrimitive)
	})
	t.Run("QueueTypeMapOfChannel", func(t *testing.T) {
		_TestParallelReader(t, QueueTypeMapOfChannel)
	})
	t.Run("QueueTypeChannel", func(t *testing.T) {
		_TestParallelReader(t, QueueTypeChannel)
	})
}

func _TestParallelReader(t *testing.T, queueType int) {
	queue := QueueFactory(queueType)

	expireTime := new(time.Time)
	*expireTime = time.Now().Add(2 * time.Second)

	go func() {
		time.Sleep(1 * time.Second)
		_, err := queue.QPopTimeout("queue", expireTime)
		if err != ErrorManyWaiterOnQueue {
			t.Errorf("Expected error, got %+v", err)
		}
	}()

	_, err := queue.QPopTimeout("queue", expireTime)
	if err != ErrorEmptyQueue {
		t.Errorf("Expected error, got %+v", err)
	}
}

func TestScenarios(t *testing.T) {
	t.Run("QueueTypePrimitive", func(t *testing.T) {
		_TestScenarios(t, QueueTypePrimitive)
	})
	t.Run("QueueTypeMapOfChannel", func(t *testing.T) {
		_TestScenarios(t, QueueTypeMapOfChannel)
	})
	t.Run("QueueTypeChannel", func(t *testing.T) {
		_TestScenarios(t, QueueTypeChannel)
	})
}

func _TestScenarios(t *testing.T, queueType int) {
	type cmdFn func(Queue)
	table := [][]cmdFn{
		[]cmdFn{
			func(q Queue) { q.QPush("key", []string{"value1"}) },
			func(q Queue) {
				str, _ := q.QPopTimeout("key", nil)
				if str != "value1" {
					t.Errorf("Test Failed")
				}
			},
		},

		[]cmdFn{
			func(q Queue) {
				zeroExpire := new(time.Time)
				*zeroExpire = time.Now().Add(0 * time.Second)
				_, err := q.QPopTimeout("key", zeroExpire)
				if err != ErrorEmptyQueue {
					t.Errorf("Expected error, got %+v", err)
				}
			},
		},

		[]cmdFn{
			func(q Queue) {
				start := time.Now()

				tenExpire := new(time.Time)
				*tenExpire = time.Now().Add(1 * time.Second)
				value, err := q.QPopTimeout("key", tenExpire)
				if err != ErrorEmptyQueue {
					t.Errorf("Expected error, got %+v", err)
				}

				if time.Since(start) < 1*time.Second {
					t.Errorf("Should wait for at least 1 seconds, returned value %+v", value)
				}
			},
		},
	}

	for _, t := range table {
		queue := QueueFactory(queueType)
		for _, fn := range t {
			fn(queue)
		}
	}
}

func TestQueueFunctionality(t *testing.T) {
	t.Run("QueueTypePrimitive", func(t *testing.T) {
		_TestQueueFunctionality(t, QueueTypePrimitive)
	})
	t.Run("QueueTypeMapOfChannel", func(t *testing.T) {
		_TestQueueFunctionality(t, QueueTypeMapOfChannel)
	})
	t.Run("QueueTypeChannel", func(t *testing.T) {
		_TestQueueFunctionality(t, QueueTypeChannel)
	})
}

func _TestQueueFunctionality(t *testing.T, queueType int) {
	queue := QueueFactory(queueType)
	values := []string{"value1", "value2", "value3"}
	queue.QPush("key1", values)

	for _, val := range values {
		v, e := queue.QPop("key1")
		if e != nil {
			t.Fatalf("Unexpected Error: %+v", e)
		}
		if v != val {
			t.Fatalf("Expected %s got %s", val, v)
		}
	}
}
