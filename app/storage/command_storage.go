package storage

import (
	"sync"
	"sync/atomic"
)

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

func GetStackCmdStruct() *CommandsStorage {
	return commandsStorage
}

func NewCommandsStorage() *CommandsStorage {
	return &CommandsStorage{
		stackOfCommands: make([]string, 10),
		mutexCmds:       &sync.RWMutex{},
	}
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
