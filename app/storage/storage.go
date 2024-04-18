package storage

import (
	"fmt"
	"sync"
	"time"

	log "github.com/codecrafters-io/redis-starter-go/app/logger"
)

type Storage interface {
	Set(key string, value string, expire int) error
	Get(key string) (string, error)
}

type InMemoryStorage struct {
	data     sync.Map
	dataTime sync.Map
}

var storage *InMemoryStorage

func init() {
	storage = NewInMemoryStorage()
}

func GetStorage() *InMemoryStorage {
	return storage
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		data:     sync.Map{},
		dataTime: sync.Map{},
	}
}

func (s *InMemoryStorage) Set(key string, value string, expire time.Time) error {
	s.data.Store(key, value)
	if !expire.IsZero() {
		s.dataTime.Store(key, expire)
	}
	return nil
}

func (s *InMemoryStorage) Get(key string) (string, error) {
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

	return value.(string), nil

}
