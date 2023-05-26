package queue

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func _BenchmarkQueuePushPop(b *testing.B, t int, goRoutineCount int, queuesName ...string) {
	b.Run(fmt.Sprintf("seqLookup-%d", goRoutineCount), func(b *testing.B) {
		q := QueueFactory(t)

		var wg sync.WaitGroup
		defer wg.Wait()

		for i := 0; i < goRoutineCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < b.N; i++ {
					queueName := queuesName[i%1000]
					q.QPush(queueName, []string{"test"})
					q.QPop(queueName)
				}
			}()
		}
	})

	b.Run(fmt.Sprintf("randLookup-%d", goRoutineCount), func(b *testing.B) {
		q := QueueFactory(t)

		var wg sync.WaitGroup
		defer wg.Wait()

		for i := 0; i < goRoutineCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < b.N; i++ {
					queueName := queuesName[rand.Intn(1000)]
					q.QPush(queueName, []string{"test"})
					q.QPop(queueName)
				}
			}()
		}
	})
}

func BenchmarkQueuePushPop(b *testing.B) {
	var queuesName [1000]string
	for i := 0; i < 1000; i++ {
		queuesName[i] = fmt.Sprintf("test-%d", i)
	}

	queueTypes := []struct {
		name string
		t    int
	}{
		{"primitive", QueueTypePrimitive},
		{"mapOfChannel", QueueTypeMapOfChannel},
		{"channelOfChannel", QueueTypeMapOfChannel},
	}

	const MaxGoroutineCount = 50
	for goRoutineCount := 5; goRoutineCount < MaxGoroutineCount; goRoutineCount += 5 {
		for _, q := range queueTypes {
			b.Run(fmt.Sprintf("%s-%d", q.name, goRoutineCount), func(b *testing.B) {
				_BenchmarkQueuePushPop(b, q.t, goRoutineCount, queuesName[:]...)
			})
		}
	}
}

func TestMapOfChannel(t *testing.T) {
	q := QueueFactory(QueueTypeMapOfChannel)
	q.QPush("test", []string{"test", "test2"})
	v, err := q.QPop("test")
	if err != nil {
		t.Fatal(err)
	}
	if v != "test" {
		t.Fatal("value mismatch")
	}

	v, err = q.QPop("test")
	if err != nil {
		t.Fatal(err)
	}
	if v != "test2" {
		t.Fatal("value mismatch")
	}
}
