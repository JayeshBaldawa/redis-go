package storage

import (
	"fmt"
	"sync"
	"time"

	log "github.com/codecrafters-io/redis-starter-go/app/logger"
)

type Storage interface {
	Set(key string, value interface{}, expire int) error
	Get(key string) (string, error)
	GetKeys() []string
}

type InMemoryStorage struct {
	data     sync.Map
	dataTime sync.Map
}

type RedisStorageInsight struct {
	offset int64
}

var storage *InMemoryStorage
var redisStorageInsight *RedisStorageInsight
var commandsStorage *CommandsStorage

func init() {
	storage = NewInMemoryStorage()
	redisStorageInsight = NewRedisStorageInsight()
	commandsStorage = NewCommandsStorage()
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		data:     sync.Map{},
		dataTime: sync.Map{},
	}
}

func NewRedisStorageInsight() *RedisStorageInsight {
	return &RedisStorageInsight{
		offset: 0,
	}
}

func GetRedisStorageInsight() *RedisStorageInsight {
	return redisStorageInsight
}

func GetStorage() *InMemoryStorage {
	return storage
}

func (s *InMemoryStorage) Set(key string, value string, expire time.Time) error {
	s.data.Store(key, value)
	if !expire.IsZero() {
		s.dataTime.Store(key, expire)
	}
	return nil
}

func (s *InMemoryStorage) Get(key string) (interface{}, error) {
	// Check if key exists
	value, ok := s.data.Load(key)
	if !ok {
		return "", nil
	}

	// Check if key has expired
	expire, ok := s.dataTime.Load(key)
	if ok {
		if time.Now().UTC().After(expire.(time.Time)) {
			log.LogError(fmt.Errorf("key %s has expired", key))
			s.data.Delete(key)
			s.dataTime.Delete(key)
			return "", nil
		}
	}

	return value, nil

}

func (s *InMemoryStorage) GetKeys() []string {
	keys := make([]string, 0)
	s.data.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	return keys
}
