package gost

import (
	"github.com/garyburd/redigo/redis"
	"sync"
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

		success := fn(item)

		if success {
			q.conn.Do("LPOP", q.Backup)
		}

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
	g.Prefix = "gost:queues"

	conn, err := redis.Dial("tcp", url)

	if err != nil {
		panic(err)
	}

	g.Redis = conn

	return g
}

func (g *Gost) createQueue(name string) *queue {
	q := new(queue)
	q.Key = g.Prefix + ":" + name
	q.Backup = q.Key + ":backup"
	q.conn = g.Redis

	return q
}

func (g *Gost) Push(queue_name string, id string) {
	queue := g.get_queue(queue_name)
	queue.push(id)
}

func (g *Gost) get_queue(queue_name string) *queue {
	queue_id := g.Prefix + ":" + queue_name

	g.mutex.Lock()
	queue := g.queues[queue_id]
	g.mutex.Unlock()

	if queue == nil {
		queue = g.createQueue(queue_name)
		g.mutex.Lock()
		g.queues[queue_id] = queue
		g.mutex.Unlock()
	}

	return queue
}

func (g *Gost) Each(queue_name string, fn caller) {
	queue := g.get_queue(queue_name)
	queue.each(fn)
}

func (g *Gost) Items(queue_name string) []string {
	queue := g.get_queue(queue_name)
	return queue.items()
}

func (g *Gost) Stop() {
	for _, queue := range g.queues {
		queue.Stop = true
	}
}
