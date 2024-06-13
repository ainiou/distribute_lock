package lock_conn

type IConn interface {
	ObtainLock(lockKey, lockVal string, ex int) (bool, error)
	ReleaseLock(lockKey, lockVal string) error
}
