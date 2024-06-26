package distribute_lock

import (
	"context"
	"fmt"
	"github.com/ainiou/distribute_lock/lock_conn"
	"github.com/garyburd/redigo/redis"
	goredis "github.com/go-redis/redis/v8"
	"testing"
	"time"
)

var pool *redis.Pool
var DL *DistributeLock
var ctx = context.Background()

func before() {

	//pool = newPool("127.0.0.1:6379", "root", 0)
	//conn := pool.Get()
	//lockConn := lock_conn.NewRedigoConn(conn)
	////defer conn.Close()
	//DL = NewDistributeLock(lockConn,
	//	WithMaxRetryTime(3),
	//	WithBackoffList([]time.Duration{time.Millisecond * 50, time.Millisecond * 500, time.Second * 2}),
	//)

	client := newClient("127.0.0.1:6379", "root", 0)
	redisConn := lock_conn.NewGoRedisConn(ctx, client)
	DL = NewDistributeLock(redisConn,
		WithMaxRetryTime(3),
		WithBackoffList([]time.Duration{time.Millisecond * 50, time.Millisecond * 500, time.Second * 2}),
	)
}

func newPool(addr, password string, db int) *redis.Pool {

	pl := &redis.Pool{
		MaxIdle:     600,
		MaxActive:   600,
		IdleTimeout: 300 * time.Second,
		Dial: func() (redis.Conn, error) {

			c, err := redis.Dial("tcp", addr,
				redis.DialDatabase(db),
				redis.DialPassword(password),
				redis.DialConnectTimeout(500*time.Millisecond),
				redis.DialReadTimeout(500*time.Millisecond),
				redis.DialWriteTimeout(500*time.Millisecond),
			)
			if err != nil {
				panic(err)
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	return pl
}

func newClient(addr, password string, db int) *goredis.Client {
	client := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	reply, err := client.Ping(ctx).Result()
	if err != nil || reply != "PONG" {
		panic(err)
	}
	return client
}

func TestDistributeLock_ObtainLock(t *testing.T) {
	before()
	lockVal := fmt.Sprintf("%d", time.Now().Nanosecond())
	obtainLock, err := DL.conn.ObtainLock("lockTest", lockVal, 1000)
	if err != nil {
		t.Errorf("err:%v", err)
		return
	}
	t.Logf("botain:%v", obtainLock)

	time.Sleep(15 * time.Second)
	err = DL.conn.ReleaseLock("lockTest", lockVal)
	if err != nil {
		t.Errorf("err:%v", err)
		return
	}
	t.Logf("release")
}

func TestDistributeLock_ReleaseLock(t *testing.T) {
	before()
	err := DL.conn.ReleaseLock("lockTest", "done")
	if err != nil {
		t.Errorf("err:%v", err)
		return
	}
	t.Logf("release")
}

func TestSet(t *testing.T) {
	before()
	conn := pool.Get()
	defer conn.Close()
	reply, err := conn.Do("SET", "lockTest2", "done", "NX", "EX", 60)
	if err != nil {
		t.Errorf("err:%v", err)
		return
	}
	t.Logf("reply:%v", reply)
}
