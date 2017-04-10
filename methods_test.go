package redikey_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"sync"

	"github.com/qianlnk/redis"
)

func TestSet(*testing.T) {
	var wg sync.WaitGroup
	for {
		for i := 0; i < 10000; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := redis.Set("test"+strconv.Itoa(i), "123", time.Second*10)
				fmt.Println(i, err)
			}(i)
		}

		wg.Wait()
	}

}

func TestGet(*testing.T) {
	var test string
	redis.Select(2)
	redis.Get("test", &test)
	fmt.Println(test)
}
