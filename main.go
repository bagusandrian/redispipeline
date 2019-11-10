package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bagusandrian/redispipeline/redis"
)

func init() {
	opt := redis.Options{
		MaxActiveConn: 100,
		MaxIdleConn:   10,
		Timeout:       3,
		Wait:          true,
	}
	redis.New("redis-local", "127.0.0.1:6379", "tcp", opt)
}
func main() {
	now := time.Now()
	singleCommandRedis()
	log.Printf("processing time singleCommandRedis: %+v\n", time.Since(now))
	time.Sleep(3 * time.Second)
	now = time.Now()
	pipelineRedis()
	log.Printf("processing time pipelineRedis: %+v\n", time.Since(now))
}

func pipelineRedis() {
	redisData := make(map[string]string)
	for i := 1; i <= 100000; i++ {
		key := fmt.Sprintf("testing_key_redis_%d", i)
		redisData[key] = fmt.Sprintf("%d", i)
	}

	result, err := redis.Pool.SetexMultiple("redis-local", redisData, 10)
	if err != nil {
		log.Printf("%+v\n", err)
	}
	log.Printf("%+v\n", result)
}

func singleCommandRedis() {
	for i := 1; i <= 100000; i++ {
		_, err := redis.Pool.Setex("redis-local", fmt.Sprintf("single_proccess_%d", i), fmt.Sprintf("%d", i), 10)
		if err != nil {
			log.Printf("%+v\n", err)
			continue
		}
	}
}
