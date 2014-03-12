package gost

import (
	"github.com/elcuervo/redisurl"
	"github.com/garyburd/redigo/redis"
	"os"
	"strconv"
	"sync"
	"time"
)

type queue struct {
	Key    string
	Backup string
	Stop   bool

	pool *redis.Pool
}

func (q *queue) push(id string) bool {
	conn := q.pool.Get()
	defer conn.Close()

	_, err := conn.Do("LPUSH", q.Key, id)

	return err == nil
}

func (q *queue) items() []string {
	conn := q.pool.Get()
	defer conn.Close()

	items, _ := redis.Strings(conn.Do("LRANGE", q.Key, 0, -1))
	return items
}

func (q *queue) size() int {
	conn := q.pool.Get()
	defer conn.Close()

	size, _ := redis.Int(conn.Do("LLEN", q.Key))
	return size
}

type caller func(string) bool

func (q *queue) each(fn caller) {
        conn := q.pool.Get()
        defer conn.Close()

	for {
		if q.Stop == true {
			break
		}

		item, err := redis.String(conn.Do("BRPOPLPUSH", q.Key, q.Backup, 5))

		if err != nil {
			continue
		}

                if success := fn(item); success {
                        conn.Do("LPOP", q.Backup)
                }

	}
}

type Gost struct {
	Prefix string
	Redis  *redis.Pool
	mutex  sync.Mutex
	queues map[string]*queue
}

func Connect(url string) *Gost {
	g := new(Gost)
	g.queues = make(map[string]*queue)
	g.Prefix = "ost"

	parsed := redisurl.Parse(url)

	conn := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", parsed.Url)
			if err != nil {
				return nil, err
			}

			if parsed.Database > 0 {
				_, err := c.Do("SELECT", parsed.Database)
				if err != nil {
					return nil, err
				}
			}

			if parsed.Password != "" {
				_, err = c.Do("AUTH", parsed.Password)
				if err != nil {
					return nil, err
				}
			}

			return c, err
		},

		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	g.Redis = conn

	return g
}

func (g *Gost) createQueue(name string) *queue {
	q := new(queue)
	hostname, _ := os.Hostname()

	q.Key = g.Prefix + ":" + name
	q.Backup = q.Key + ":" + hostname + ":" + strconv.Itoa(os.Getpid())
	q.pool = g.Redis

	return q
}

func (g *Gost) Push(queueName string, id string) {
	queue := g.getQueue(queueName)
	queue.push(id)
}

func (g *Gost) getQueue(queueName string) *queue {
	queueId := g.Prefix + ":" + queueName

	g.mutex.Lock()
	queue := g.queues[queueId]
	g.mutex.Unlock()

	if queue == nil {
		queue = g.createQueue(queueName)
		g.mutex.Lock()
		g.queues[queueId] = queue
		g.mutex.Unlock()
	}

	return queue
}

func (g *Gost) Each(queueName string, fn caller) {
	queue := g.getQueue(queueName)
	queue.each(fn)
}

func (g *Gost) Items(queueName string) []string {
	queue := g.getQueue(queueName)
	return queue.items()
}

func (g *Gost) Stop() {
	for _, queue := range g.queues {
		queue.Stop = true
	}
}

func (g *Gost) Size(queueName string) int {
	queue := g.getQueue(queueName)
	return queue.size()
}
