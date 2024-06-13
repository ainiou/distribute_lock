package distribute_lock

import (
	"errors"
	"github.com/ainiou/distribute_lock/lock_conn"
	"github.com/ainiou/distribute_lock/retry"
	"time"
)

type DistributeLock struct {
	conn         lock_conn.IConn
	maxRetryTime int
	BackoffList  []time.Duration
}

type Option func(d *DistributeLock)

func defaultConfig(conn lock_conn.IConn) *DistributeLock {
	return &DistributeLock{
		conn:         conn,
		maxRetryTime: 3,
		BackoffList:  []time.Duration{time.Millisecond * 50, time.Millisecond * 500, time.Second * 2},
	}
}

func NewDistributeLock(conn lock_conn.IConn, options ...Option) *DistributeLock {
	l := defaultConfig(conn)
	for _, option := range options {
		option(l)
	}
	return l
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
	getLock, _ := d.conn.ObtainLock(lockKey, lockVal, ex)
	if getLock {
		return true, nil
	}

	var err error
	err = retry.RetryFunc(func() error {
		getLock, _ = d.conn.ObtainLock(lockKey, lockVal, ex)
		if getLock {
			return nil
		}
		return err
	}, retry.WithMaxRetry(d.maxRetryTime), retry.WithBackoff(d.BackoffList...))
	if err == nil {
		return true, nil
	}
	return false, err
}

func (d *DistributeLock) ReleaseLock(lockKey, lockVal string) error {
	if d.conn == nil {
		return errors.New("连接为空")
	}
	err := d.conn.ReleaseLock(lockKey, lockVal)
	if err != nil {
		return err
	}
	return nil
}
