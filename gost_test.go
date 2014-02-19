package gost

import (
	"git.tideland.biz/gots/asserts"
	"github.com/garyburd/redigo/redis"
	"testing"
	"time"
)

func init() {
	conn, _ := redis.Dial("tcp", ":6379")
	conn.Do("FLUSHDB")
}

func TestAccessToQueue(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	g := Connect(":6379")

	g.Push("my_queue", "1")
	g.Push("my_queue", "2")

	items := []string{"2", "1"}

	assert.Equal(g.Items("my_queue"), items)
}

func TestReadingQueue(t *testing.T) {
	g := Connect(":6379")
	g.Prefix = "test:queues"

	g.Push("my_queue", "1")

	go func() {
		time.Sleep(time.Millisecond * 500)
		g.Stop()
	}()

	g.Each("my_queue", func(id string) bool {
		return true
	})

}

func TestQueueSize(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	g := Connect(":6379")

	c := make(chan string, 1)

	assert.Equal(g.Size("my_queue2"), 0)

	g.Push("my_queue2", "1")

	assert.Equal(g.Size("my_queue2"), 1)

	go func() {
		time.Sleep(time.Millisecond * 1000)
		g.Stop()
	}()

	g.Each("my_queue2", func(id string) bool {
		c <- id
		return true
	})

	<-c

	assert.Equal(g.Size("my_queue2"), 0)
}
