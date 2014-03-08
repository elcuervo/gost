package gost

import (
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func init() {
	conn, _ := redis.Dial("tcp", ":6379")
	conn.Do("SELECT", 6)
	conn.Do("FLUSHDB")
}

func TestAccessToQueue(t *testing.T) {
	g := Connect(":6379/6")

	g.Push("my_queue", "1")
	g.Push("my_queue", "2")

	items := []string{"2", "1"}

	assert.Equal(t, items, g.Items("my_queue"))
}

func TestReadingQueue(t *testing.T) {
	g := Connect(":6379/6")
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
