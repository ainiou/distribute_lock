package distribute_lock

import (
	"github.com/garyburd/redigo/redis"
	"testing"
	"time"
	"zonst/qipai/api/clubbaseinfoapisrv/utils"
)

var pool *redis.Pool
var DL *DistributeLock

func before() {

	pool = utils.NewPool("127.0.0.1:6379", "root", 0)
	conn := pool.Get()
	//defer conn.Close()
	DL = NewDistributeLock(conn,
		WithMaxRetryTime(3),
		WithBackoffList([]time.Duration{time.Millisecond * 50, time.Millisecond * 500, time.Second * 2}),
	)
}
func TestDistributeLock_ObtainLock(t *testing.T) {
	before()
	obtainLock, err := DL.ObtainLock("lockTest", 1000)
	if err != nil {
		t.Errorf("err:%v", err)
		return
	}
	t.Logf("botain:%v", obtainLock)
}

func TestDistributeLock_ReleaseLock(t *testing.T) {
	before()
	err := DL.ReleaseLock("lockTest")
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
