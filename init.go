package redikey

import (
	"log"
	"sync"
	"time"
)

var (
	myRedis           RedisCache
	defaultExpiration = time.Hour
	once              sync.Once
)

func init() {
	once.Do(initializing)
}

func initializing() {
	host := "127.0.0.1:6379"
	var err error
	myRedis, err = newRedisCache(host, "", 2, defaultExpiration)
	if err != nil {
		log.Fatal(err)
	}
}
