package lock_conn

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"time"
)

type GoRedisConn struct {
	conn *redis.Client
	ctx  context.Context
}

func NewGoRedisConn(ctx context.Context, conn *redis.Client) *GoRedisConn {
	return &GoRedisConn{conn: conn, ctx: ctx}
}

func (d *GoRedisConn) ObtainLock(lockKey, lockVal string, ex int) (bool, error) {
	if d.conn == nil {
		return false, errors.New("conn is nil")
	}
	ok, err := d.conn.SetNX(d.ctx, lockKey, lockVal, time.Second*time.Duration(ex)).Result()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, errors.New("未获取到锁")
	}
	return true, nil
}

func (d *GoRedisConn) ReleaseLock(lockKey, lockVal string) error {
	if d.conn == nil {
		return errors.New("conn is nil")
	}
	_, err := d.conn.Eval(d.ctx, ReleaseScript, []string{lockKey}, lockVal).Result()
	if err != nil {
		return err
	}
	return nil
}
