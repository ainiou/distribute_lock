package distribute_lock

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
	"zonst/qipai/api/clubbaseinfoapisrv/utils/retry"
)

const (
	ReleaseScript = `
	if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
`
)

type DistributeLock struct {
	conn         redis.Conn
	maxRetryTime int
	BackoffList  []time.Duration
}

type Option func(d *DistributeLock)

func defaultConfig(conn redis.Conn) *DistributeLock {
	return &DistributeLock{
		conn:         conn,
		maxRetryTime: 3,
		BackoffList:  []time.Duration{time.Millisecond * 50, time.Millisecond * 500, time.Second * 2},
	}
}

func NewDistributeLock(conn redis.Conn, options ...Option) *DistributeLock {
	lock := defaultConfig(conn)
	for _, option := range options {
		option(lock)
	}
	return lock
}

func WithMaxRetryTime(maxRetryTime int) Option {
	return func(d *DistributeLock) {
		d.maxRetryTime = maxRetryTime
	}
}

func WithBackoffList(backoffList []time.Duration) Option {
	return func(d *DistributeLock) {
		d.BackoffList = backoffList
	}
}

func (d *DistributeLock) ObtainLock(lockKey, lockVal string, ex int) (bool, error) {
	if d.conn == nil {
		return false, errors.New("连接为空")
	}
	// 尝试获取
	getLock, _ := d.lock(lockKey, lockVal, ex)
	if getLock {
		return true, nil
	}

	var err error
	err = retry.RetryFunc(func() error {
		lock, _ := d.lock(lockKey, lockVal, ex)
		if lock {
			return nil
		}
		return err
	}, retry.WithMaxRetry(d.maxRetryTime), retry.WithBackoff(d.BackoffList...))
	if err == nil {
		return true, nil
	}
	return false, err
}

func (d *DistributeLock) lock(lockKey string, lockVal string, ex int) (bool, error) {
	// 尝试获取
	reply, err := redis.String(d.conn.Do("SET", lockKey, lockVal, "NX", "EX", ex))
	// 测试发现存在也不会有错误，而是 reply = nil
	if err != nil && !errors.Is(err, redis.ErrNil) {
		return false, err
	}
	// 拿到了锁
	if reply == "OK" {
		return true, nil
	}
	// 没拿到锁
	return false, errors.New(fmt.Sprintf("未获取到锁:%s", err.Error()))
}

func (d *DistributeLock) ReleaseLock(lockKey, lockVal string) error {
	if d.conn == nil {
		return errors.New("连接为空")
	}
	reply, err := d.conn.Do("EVAL", ReleaseScript, 1, lockKey, lockVal)
	if err != nil {
		return err
	}
	// 这下面的逻辑也可以不处理。发生如下逻辑说明发生了激烈的锁竞争 即上一个线程的处理时间超过了锁的过期时间(EX) key 自动过期且其他线程获取到锁。
	// 这时第一个线程释放锁时 get and compare 失败，del 删除0个key
	if reply.(int64) != 1 {
		return errors.New(fmt.Sprintf("释放锁失败:key:%s,val:%s", lockKey, lockVal))
	}
	return nil
}
