package queue

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

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

	const GoRoutineCount = 20

	for _, q := range queueTypes {
		b.Run(fmt.Sprintf("%s-seqLookup-%d", q.name, GoRoutineCount), func(b *testing.B) {
			q := QueueFactory(q.t)

			var wg sync.WaitGroup
			defer wg.Wait()

			for i := 0; i < GoRoutineCount; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < b.N; i++ {
						queueName := queuesName[i%1000]
						q.QPush(queueName, []string{"test"})
						_, e := q.QPop(queueName)
						if e != nil {
							b.Fatal(e)
						}
					}
				}()
			}
		})

		b.Run(fmt.Sprintf("%s-randLookup-%d", q.name, GoRoutineCount), func(b *testing.B) {
			q := QueueFactory(QueueTypePrimitive)

			var wg sync.WaitGroup
			defer wg.Wait()

			for i := 0; i < GoRoutineCount; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < b.N; i++ {
						queueName := queuesName[rand.Intn(1000)]
						q.QPush(queueName, []string{"test"})
						_, e := q.QPop(queueName)
						if e != nil {
							b.Fatal(e)
						}
					}
				}()
			}
		})
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
