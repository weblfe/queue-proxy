package dq

import "github.com/tal-tech/go-zero/core/stores/redis"

type (
	Beanstalk struct {
		Endpoint string
		Tube     string
	}

	BreakerConf struct {
		Beanstalks []Beanstalk
		Redis      redis.RedisConf
	}
)
