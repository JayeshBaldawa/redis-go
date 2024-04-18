package commands

import (
	"errors"
	"fmt"
	"strings"

	parserModel "github.com/codecrafters-io/redis-starter-go/app/models"
	config "github.com/codecrafters-io/redis-starter-go/app/utility"
)

type SlaveParser struct{}

func (slaveParser *SlaveParser) ProcessArrayCommand(strCommand []string, numElements int) (string, error) {
	// Ensure at least one command is provided
	if len(strCommand) < 1 {
		return "", errors.New("no command provided")
	}

	// Convert command to lower case for case-insensitive comparison
	command := strings.ToLower(strCommand[0])

	switch command {
	case parserModel.ECHO_COMMAND:
		return encodeBulkString(getCommandParameter(strCommand, 1)), nil
	case parserModel.PING_COMMAND:
		return encodeSimpleString("PONG"), nil
	case parserModel.INFO_COMMAND:
		return slaveParser.processInfoCommand(strCommand)
	default:
		return "", errors.New("unknown command")
	}
}

func (slaveParser *SlaveParser) processInfoCommand(strCommand []string) (string, error) {
	if len(strCommand) > 1 && strings.ToLower(strCommand[1]) == parserModel.INFO_REPLICATION {
		return encodeBulkString(fmt.Sprintf(parserModel.REPLICATION, config.GetRedisServerConfig().GetServerType())), nil
	}
	return "", errors.New("invalid format for INFO command")
}
