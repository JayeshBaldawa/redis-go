package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/codecrafters-io/redis-starter-go/app/logger"
)

type Storage interface {
	Set(key string, value interface{}, expire int) error
	Get(key string) (string, error)
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

func NewCommandsStorage() *CommandsStorage {
	return &CommandsStorage{
		stackOfCommands: make([]string, 10),
		mutexCmds:       &sync.RWMutex{},
	}
}

func GetRedisStorageInsight() *RedisStorageInsight {
	return redisStorageInsight
}

func GetStorage() *InMemoryStorage {
	return storage
}

func GetStackCmdStruct() *CommandsStorage {
	return commandsStorage
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

type CommandsStorage struct {
	// Stack of last 10 commands
	stackOfCommands []string
	mutexCmds       *sync.RWMutex
}

func (r *RedisStorageInsight) Set(offset int64) {
	atomic.AddInt64(&r.offset, offset)
}

func (r *RedisStorageInsight) Get() int64 {
	return atomic.LoadInt64(&r.offset)
}

func (c *CommandsStorage) AddCommand(command string) {
	c.mutexCmds.Lock()
	defer c.mutexCmds.Unlock()
	c.stackOfCommands = append(c.stackOfCommands, command)
	if len(c.stackOfCommands) > 10 {
		c.stackOfCommands = c.stackOfCommands[1:]
	}
}

func (c *CommandsStorage) GetTopOfStack() string {
	c.mutexCmds.RLock()
	defer c.mutexCmds.RUnlock()
	return c.stackOfCommands[len(c.stackOfCommands)-1]
}

func (c *CommandsStorage) ClearCommands() {
	c.mutexCmds.Lock()
	defer c.mutexCmds.Unlock()
	c.stackOfCommands = make([]string, 10)
}
