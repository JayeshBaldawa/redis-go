package commands

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	parserModel "github.com/codecrafters-io/redis-starter-go/app/models"
	storage "github.com/codecrafters-io/redis-starter-go/app/storage"
	config "github.com/codecrafters-io/redis-starter-go/app/utility"
)

type MasterParser struct{}

func (masterParser *MasterParser) ProcessArrayCommand(strCommand []string, numElements int) (string, error) {
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
	case parserModel.SET_COMMAND:
		return masterParser.processSetCommand(strCommand, numElements)
	case parserModel.GET_COMMAND:
		return masterParser.processGetCommand(strCommand)
	case parserModel.INFO_COMMAND:
		return masterParser.processInfoCommand(strCommand)
	case parserModel.REPLCONF:
		return masterParser.checkReplconCommand(strCommand)
	case parserModel.PYSNC:
		return masterParser.handlePysncCommand(), nil
	default:
		return "", errors.New("unknown command")
	}
}

func (masterParser *MasterParser) handlePysncCommand() string {
	return encodeSimpleString(parserModel.FULLRESYNC + " 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0")
}

func (masterParser *MasterParser) checkReplconCommand(strCommand []string) (string, error) {
	if len(strCommand) < 3 {
		return "", errors.New("invalid format for REPLCONF command")
	}
	switch strings.ToLower(strCommand[1]) {
	case parserModel.REPLCONF_LISTEN_PORT:
		return encodeSimpleString("OK"), nil
	case parserModel.REPLCONF_CAPA:
		if strings.ToLower(strCommand[2]) == parserModel.REPLCONF_PYSYNC2 {
			return encodeSimpleString("OK"), nil
		}
	}
	return "", errors.New("invalid format for REPLCONF command")
}

func (masterParser *MasterParser) processSetCommand(strCommand []string, numElements int) (string, error) {
	if numElements != 5 && numElements != 3 {
		return "", errors.New("invalid format for SET command")
	}

	var timeOfExpiry time.Time
	if numElements == 5 {
		expire, err := strconv.Atoi(strCommand[4])
		if err != nil {
			return "", errors.New("invalid format for expiry time")
		}
		timeOfExpiry = getExpiryTimeInUTC(expire, strCommand[3])
		if timeOfExpiry.IsZero() {
			return "", errors.New("invalid format for time type")
		}
	}

	err := storage.GetStorage().Set(strCommand[1], strCommand[2], timeOfExpiry)
	if err != nil {
		return "", err
	}

	return encodeSimpleString("OK"), nil
}

func (masterParser *MasterParser) processGetCommand(strCommand []string) (string, error) {
	if len(strCommand) < 2 {
		return "", errors.New("invalid format for GET command")
	}

	value, err := storage.GetStorage().Get(strCommand[1])
	if err != nil {
		return "", err
	}

	if value == "" {
		return encodeNullBulkString(), nil
	}

	return encodeBulkString(value), nil
}

func (masterParser *MasterParser) processInfoCommand(strCommand []string) (string, error) {
	if len(strCommand) > 1 && strings.ToLower(strCommand[1]) == parserModel.INFO_REPLICATION {
		return encodeBulkString(fmt.Sprintf(parserModel.REPLICATION, config.GetRedisServerConfig().GetServerType())), nil
	}
	return "", errors.New("invalid format for INFO command")
}

func getCommandParameter(strCommand []string, index int) string {
	if index >= len(strCommand) {
		return ""
	}
	return strCommand[index]
}
