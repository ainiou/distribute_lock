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

func (d *DistributeLock) ObtainLock(lockKey string, ex int) (bool, error) {
	if d.conn == nil {
		return false, errors.New("连接为空")
	}
	// 尝试获取
	getLock, _ := d.lock(lockKey, ex)
	if getLock {
		return true, nil
	}

	var err error
	err = retry.RetryFunc(func() error {
		lock, _ := d.lock(lockKey, ex)
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

func (d *DistributeLock) lock(lockKey string, ex int) (bool, error) {
	// 尝试获取
	reply, err := redis.String(d.conn.Do("SET", lockKey, "done", "NX", "EX", ex))
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

func (d *DistributeLock) ReleaseLock(lockKey string) error {
	if d.conn == nil {
		return errors.New("连接为空")
	}
	_, err := d.conn.Do("EVAL", ReleaseScript, 1, lockKey, "done")
	if err != nil {
		return err
	}
	return nil
}
