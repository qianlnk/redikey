package redikey

import (
	"time"
)

func Ping() error {
	return myRedis.ping()
}

func Select(db int) error {
	var err error
	myRedis, err = newRedisCache(myRedis.host+":"+myRedis.port, "", db, defaultExpiration)
	return err
}
func Set(key string, value interface{}, expires time.Duration) error {
	return myRedis.set(key, value, expires)
}

func Add(key string, value interface{}, expires time.Duration) error {
	return myRedis.add(key, value, expires)
}

func Replace(key string, value interface{}, expires time.Duration) error {
	return myRedis.replace(key, value, expires)
}

func Get(key string, ptrValue interface{}) error {
	return myRedis.get(key, ptrValue)
}

func GetMulti(keys ...string) (Getter, error) {
	return myRedis.getMulti(keys...)
}

func Exists(key string) (bool, error) {
	return myRedis.exists(key)
}

func Delete(key string) error {
	return myRedis.delete(key)
}

func TTL(key string) (int64, error) {
	return myRedis.ttl(key)
}

func SADD(key string, values ...interface{}) error {
	return myRedis.sadd(key, values...)
}

func ZADD(key string, values ...interface{}) error {
	return myRedis.zadd(key, values...)
}

func ZRANGE(key string, start, end int) ([]string, error) {
	return myRedis.zrange(key, start, end)
}

func SORT(key string, values ...interface{}) ([]string, error) {
	return myRedis.sort(key, values...)
}

func SMEMBERS(key string) ([]string, error) {
	return myRedis.smembers(key)
}

func KEYS(keyformat string) ([]string, error) {
	return myRedis.keys(keyformat)
}

func HSET(key, field string, value interface{}) error {
	return myRedis.hset(key, field, value)
}

func HGET(key, field string, ptrValue interface{}) error {
	return myRedis.hget(key, field, ptrValue)
}

func HKEYS(key string) ([]string, error) {
	return myRedis.hkeys(key)
}

func PUSH(key string, value interface{}) error {
	return myRedis.push(key, value)
}

func POP(key string, ptrValue interface{}) error {
	return myRedis.pop(key, ptrValue)
}

func DeleteKeyFormat(keyFormat string) ([]byte, error) {
	return myRedis.deleteKeyFormat(keyFormat)
}

func Increment(key string, delta uint64) (uint64, error) {
	return myRedis.increment(key, delta)
}

func IncrementFloat(key string, delta float64) (float64, error) {
	return myRedis.incrementFloat(key, delta)
}

func Decrement(key string, delta uint64) (newValue uint64, err error) {
	return myRedis.decrement(key, delta)
}

func Flush() error {
	return myRedis.flush()
}
