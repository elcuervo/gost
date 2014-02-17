package gost

import (
	"github.com/garyburd/redigo/redis"
	"sync"
        "os"
        "strconv"
)

type queue struct {
	Key    string
	Backup string
	Stop   bool

	conn redis.Conn
}

func (q *queue) push(id string) {
	q.conn.Do("LPUSH", q.Key, id)
}

func (q *queue) items() []string {
	items, _ := redis.Strings(q.conn.Do("LRANGE", q.Key, 0, -1))
	return items
}

type caller func(string) bool

func (q *queue) each(fn caller) {
	for {
		if q.Stop == true {
			break
		}

		item, err := redis.String(q.conn.Do("BRPOPLPUSH", q.Key, q.Backup, 2))

		if err != nil {
			continue
		}

		go func() {
			if success := fn(item); success {
				q.conn.Do("LPOP", q.Backup)
			}
		}()

	}
}

type Gost struct {
	Prefix string
	Redis  redis.Conn
	mutex  sync.Mutex
	queues map[string]*queue
}

func Connect(url string) *Gost {
	g := new(Gost)
	g.queues = make(map[string]*queue)
	g.Prefix = "ost"

	conn, err := redis.Dial("tcp", url)

	if err != nil {
		panic(err)
	}

	g.Redis = conn

	return g
}

func (g *Gost) createQueue(name string) *queue {
	q := new(queue)
        hostname, _ := os.Hostname()

	q.Key = g.Prefix + ":" + name
        q.Backup = q.Key + ":" + hostname + ":" + strconv.Itoa(os.Getpid())
	q.conn = g.Redis

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
