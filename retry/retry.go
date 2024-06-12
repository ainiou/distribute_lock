package retry

import "time"

type config struct {
	MaxRetryTime int
	BackoffList  []time.Duration
}

func (c config) getRetryBackoff(times int) time.Duration {
	if times < 0 || len(c.BackoffList) <= 0 {
		return 0
	}
	bk := c.BackoffList
	if times < len(bk) {
		return bk[times]
	}
	return bk[len(bk)-1]
}

type Option func(c *config)

func WithMaxRetry(maxRetry int) Option {
	return func(c *config) {
		c.MaxRetryTime = maxRetry
	}
}

func WithBackoff(backoffs ...time.Duration) Option {
	return func(c *config) {
		c.BackoffList = backoffs
	}
}

func WithCommonConfig() Option {
	return func(c *config) {
		c.MaxRetryTime = 3
		c.BackoffList = []time.Duration{time.Millisecond * 50, time.Millisecond * 500, time.Second * 2}
	}
}

func defaultConfig() config {
	return config{
		MaxRetryTime: 3,
		BackoffList:  []time.Duration{},
	}
}

// RetryFunc 重试
func RetryFunc(fn func() error, opts ...Option) error {
	c := defaultConfig()
	for _, o := range opts {
		o(&c)
	}

	var err error
	for i := 0; i < c.MaxRetryTime; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		t := c.getRetryBackoff(i)
		if t > 0 {
			time.Sleep(t)
		}
	}
	return err
}
