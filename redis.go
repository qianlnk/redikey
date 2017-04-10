/*
	Redis high traffic connection issue
	This TCP states is when your system is negotiating a "graceful" disconnect with the other end.
	TIME_WAIT means both sides have agreed to close and TCP must now wait a prescribed time before
	taking the connection down.

	Solution on Linux is:

		echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse
		echo 1 > /proc/sys/net/ipv4/tcp_tw_recycle

	This allow the OS to quickly reuse those TIME_WAIT TCP sockets.

	Solution on MacOSX is:
		sysctl -w net.inet.tcp.msl=1000

*/
package redikey

import (
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	DEFAULT = time.Duration(0)
	FOREVER = time.Duration(-1)
)

var (
	ErrCacheMiss = errors.New("revel/cache: key not found.")
	ErrNotStored = errors.New("revel/cache: not stored.")
)

// Wraps the Redis client to meet the Cache interface.
type RedisCache struct {
	pool              *redis.Pool
	host              string
	port              string
	defaultExpiration time.Duration
}

// Getter is an interface for getting / decoding an element from a cache.
type Getter interface {
	Get(key string, ptrValue interface{}) error
}

// until redigo supports sharding/clustering, only one host will be in hostList
func newRedisCache(host string, password string, db int, defaultExpiration time.Duration) (RedisCache, error) {
	// var pool = &redis.Pool{
	// 	MaxActive:   10000,
	// 	MaxIdle:     5,
	// 	IdleTimeout: 120 * time.Second,
	// 	Dial: func() (redis.Conn, error) {
	// 		optionDb := redis.DialDatabase(db)
	// 		optionPwd := redis.DialPassword(password)
	// 		protocol := "tcp"
	// 		c, err := redis.Dial(protocol, host, optionDb, optionPwd)
	// 		if err != nil {
	// 			return nil, err
	// 		}

	// 		// check with PING
	// 		if _, err := c.Do("PING"); err != nil {
	// 			c.Close()
	// 			return nil, err
	// 		}

	// 		return c, err
	// 	},
	// 	// custom connection test method
	// 	TestOnBorrow: func(c redis.Conn, t time.Time) error {
	// 		if _, err := c.Do("PING"); err != nil {
	// 			return err
	// 		}
	// 		return nil
	// 	},
	// }

	rp := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host)
			if err != nil {
				return nil, err
			}

			c.Do("SELECT", 2)
			return c, err
		},

		MaxIdle:     50,
		MaxActive:   500,
		IdleTimeout: time.Second * 180,
	}
	addrs := strings.Split(host, ":")
	if len(addrs) < 2 {
		return RedisCache{nil, "", "", defaultExpiration}, errors.New("redis host is error!")
	}
	return RedisCache{rp, addrs[0], addrs[1], defaultExpiration}, nil
}

func (c RedisCache) selectDB(db int) {
	conn := c.pool.Get()
	defer conn.Close()
	conn.Do("SELECT", db)
}

func (c RedisCache) deleteKeyFormat(keyformat string) ([]byte, error) {
	cmd := fmt.Sprintf("redis-cli -h %s -p %s KEYS "+keyformat+" | xargs --delim='\n' redis-cli -h %s -p %s DEL prevent_empty_key", c.host, c.port, c.host, c.port)
	// xarg on osx doesn't support -d option, use -n instead
	if runtime.GOOS == "darwin" {
		cmd = fmt.Sprintf("redis-cli -h %s -p %s KEYS "+keyformat+" | xargs -n 1 redis-cli -h %s -p %s DEL prevent_empty_key", c.host, c.port, c.host, c.port)
	}
	return exec.Command("bash", "-c", cmd).Output()
}

func (c RedisCache) set(key string, value interface{}, expires time.Duration) error {
	conn := c.pool.Get()
	defer conn.Close()
	return c.invoke(conn.Do, key, value, expires)
}

func (c RedisCache) add(key string, value interface{}, expires time.Duration) error {
	conn := c.pool.Get()
	defer conn.Close()
	existed, err := exists(conn, key)
	if err != nil {
		return err
	} else if existed {
		return ErrNotStored
	}
	return c.invoke(conn.Do, key, value, expires)
}

func (c RedisCache) replace(key string, value interface{}, expires time.Duration) error {
	conn := c.pool.Get()
	defer conn.Close()
	existed, err := exists(conn, key)
	if err != nil {
		return err
	} else if !existed {
		return ErrNotStored
	}
	err = c.invoke(conn.Do, key, value, expires)
	if value == nil {
		return ErrNotStored
	} else {
		return err
	}
}

func (c RedisCache) get(key string, ptrValue interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	raw, err := conn.Do("GET", key)
	if err != nil {
		return err
	} else if raw == nil {
		return ErrCacheMiss
	}
	item, err := redis.Bytes(raw, err)
	if err != nil {
		return err
	}
	return Deserialize(item, ptrValue)
}

func generalizeStringSlice(strs []string) []interface{} {
	ret := make([]interface{}, len(strs))
	for i, str := range strs {
		ret[i] = str
	}
	return ret
}

func (c RedisCache) getMulti(keys ...string) (Getter, error) {
	conn := c.pool.Get()
	defer conn.Close()

	items, err := redis.Values(conn.Do("MGET", generalizeStringSlice(keys)...))
	if err != nil {
		return nil, err
	} else if items == nil {
		return nil, ErrCacheMiss
	}

	m := make(map[string][]byte)
	for i, key := range keys {
		m[key] = nil
		if i < len(items) && items[i] != nil {
			s, ok := items[i].([]byte)
			if ok {
				m[key] = s
			}
		}
	}
	return RedisItemMapGetter(m), nil
}

func exists(conn redis.Conn, key string) (bool, error) {
	return redis.Bool(conn.Do("EXISTS", key))
}

func (c RedisCache) exists(key string) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	return exists(conn, key)
}

func (c RedisCache) delete(key string) error {
	conn := c.pool.Get()
	defer conn.Close()
	existed, err := redis.Bool(conn.Do("DEL", key))
	if err == nil && !existed {
		err = ErrCacheMiss
	}
	return err
}

func (c RedisCache) ttl(key string) (int64, error) {
	conn := c.pool.Get()
	defer conn.Close()
	existed, err := exists(conn, key)
	if err != nil {
		return 0, err
	} else if !existed {
		return 0, ErrNotStored
	}
	ttl, err := redis.Int64(conn.Do("TTL", key))
	if err == nil {
		return 0, err
	}
	return ttl, nil
}

func (c RedisCache) sadd(key string, values ...interface{}) error {
	var params []interface{}
	conn := c.pool.Get()
	defer conn.Close()
	params = append(params, key)
	params = append(params, values...)
	_, err := conn.Do("SADD", params...)
	if err != nil {
		return err
	}
	return nil
}

func (c RedisCache) smembers(key string) ([]string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	return redis.Strings(conn.Do("SMEMBERS", key))
}

func (c RedisCache) zadd(key string, values ...interface{}) error {
	var params []interface{}
	conn := c.pool.Get()
	defer conn.Close()
	params = append(params, key)
	params = append(params, values...)
	_, err := conn.Do("ZADD", params...)
	if err != nil {
		return err
	}
	return nil
}

func (c RedisCache) zrange(key string, start, end int) ([]string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	return redis.Strings(conn.Do("ZRANGE", key, start, end))
}

func (c RedisCache) sort(key string, values ...interface{}) ([]string, error) {
	var params []interface{}
	conn := c.pool.Get()
	defer conn.Close()
	params = append(params, key)
	params = append(params, values...)
	return redis.Strings(conn.Do("SORT", params...))
}
func (c RedisCache) keys(keyformat string) ([]string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	return redis.Strings(conn.Do("KEYS", keyformat))
}

func (c RedisCache) hset(key, field string, value interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	b, err := Serialize(value)
	if err != nil {
		return err
	}
	_, err = conn.Do("HSET", key, field, b)
	if err == nil {
		return err
	}
	return nil
}

func (c RedisCache) hget(key, field string, ptrValue interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	raw, err := conn.Do("HGET", key, field)
	if err != nil {
		return err
	} else if raw == nil {
		return ErrCacheMiss
	}
	item, err := redis.Bytes(raw, err)
	if err != nil {
		return err
	}
	return Deserialize(item, ptrValue)
}

func (c RedisCache) hkeys(key string) ([]string, error) {
	conn := c.pool.Get()
	defer conn.Close()

	return redis.Strings(conn.Do("HKEYS", key))
}

func (c RedisCache) push(key string, value interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	b, err := Serialize(value)
	if err != nil {
		return err
	}
	_, err = conn.Do("RPUSH", key, b)
	if err == nil {
		return err
	}
	return nil
}

func (c RedisCache) pop(key string, ptrValue interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	raw, err := conn.Do("LPOP", key)
	if err != nil {
		return err
	} else if raw == nil {
		return ErrCacheMiss
	}
	item, err := redis.Bytes(raw, err)
	if err != nil {
		return err
	}
	return Deserialize(item, ptrValue)
}

func (c RedisCache) increment(key string, delta uint64) (uint64, error) {
	conn := c.pool.Get()
	defer conn.Close()
	// Check for existance *before* increment as per the cache contract.
	// redis will auto create the key, and we don't want that. Since we need to do increment
	// ourselves instead of natively via INCRBY (redis doesn't support wrapping), we get the value
	// and do the exists check this way to minimize calls to Redis
	existed, err := exists(conn, key)
	if err != nil {
		return 0, err
	} else if !existed {
		return 0, ErrCacheMiss
	}

	val, err := redis.Int64(conn.Do("INCRBY", key, delta))
	if err != nil {
		return 0, err
	}
	return uint64(val), nil
}

func (c RedisCache) incrementFloat(key string, delta float64) (float64, error) {
	conn := c.pool.Get()
	defer conn.Close()
	// Check for existance *before* increment as per the cache contract.
	// redis will auto create the key, and we don't want that. Since we need to do increment
	// ourselves instead of natively via INCRBY (redis doesn't support wrapping), we get the value
	// and do the exists check this way to minimize calls to Redis
	existed, err := exists(conn, key)
	if err != nil {
		return 0, err
	} else if !existed {
		return 0, ErrCacheMiss
	}

	val, err := redis.Float64(conn.Do("INCRBYFLOAT", key, delta))
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (c RedisCache) decrement(key string, delta uint64) (newValue uint64, err error) {
	conn := c.pool.Get()
	defer conn.Close()
	// Check for existance *before* increment as per the cache contract.
	// redis will auto create the key, and we don't want that, hence the exists call
	existed, err := exists(conn, key)
	if err != nil {
		return 0, err
	} else if !existed {
		return 0, ErrCacheMiss
	}
	// Decrement contract says you can only go to 0
	// so we go fetch the value and if the delta is greater than the amount,
	// 0 out the value
	currentVal, err := redis.Int64(conn.Do("GET", key))
	if err != nil {
		return 0, err
	}
	if delta > uint64(currentVal) {
		tempint, err := redis.Int64(conn.Do("DECRBY", key, currentVal))
		return uint64(tempint), err
	}
	tempint, err := redis.Int64(conn.Do("DECRBY", key, delta))
	return uint64(tempint), err
}

func (c RedisCache) flush() error {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("FLUSHALL")
	return err
}

func (c RedisCache) invoke(f func(string, ...interface{}) (interface{}, error),
	key string, value interface{}, expires time.Duration) error {

	switch expires {
	case DEFAULT:
		expires = c.defaultExpiration
	case FOREVER:
		expires = time.Duration(0)
	}

	b, err := Serialize(value)
	if err != nil {
		return err
	}
	conn := c.pool.Get()
	defer conn.Close()
	if expires > 0 {
		_, err := f("SETEX", key, int32(expires/time.Second), b)
		return err
	} else {
		_, err := f("SET", key, b)
		return err
	}
}

func (c RedisCache) ping() error {
	conn := c.pool.Get()
	defer conn.Close()

	_, err := conn.Do("PING")
	return err
}

// Implement a Getter on top of the returned item map.
type RedisItemMapGetter map[string][]byte

func (g RedisItemMapGetter) Get(key string, ptrValue interface{}) error {
	item, ok := g[key]
	if !ok {
		return ErrCacheMiss
	}
	return Deserialize(item, ptrValue)
}
