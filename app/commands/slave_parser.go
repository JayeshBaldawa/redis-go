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

type SlaveParser struct{}

func (slaveParser *SlaveParser) ProcessArrayCommand(strCommand []string, numElements int) (parserModel.CommandOutput, error) {
	// Ensure at least one command is provided
	if len(strCommand) < 1 {
		return parserModel.CommandOutput{}, errors.New("no command provided")
	}

	// Convert command to lower case for case-insensitive comparison
	command := strings.ToLower(strCommand[0])

	switch command {
	case parserModel.ECHO_COMMAND:
		return formatCommandOutput(encodeBulkString(getCommandParameter(strCommand, 1)), parserModel.ECHO_COMMAND, nil), nil
	case parserModel.PING_COMMAND:
		return formatCommandOutput(encodeSimpleString("PONG"), parserModel.PING_COMMAND, nil), nil
	case parserModel.INFO_COMMAND:
		resp, err := slaveParser.processInfoCommand(strCommand)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.INFO_COMMAND, nil), nil
	case parserModel.SET_COMMAND:
		resp, err := slaveParser.processSetCommand(strCommand, numElements)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.SET_COMMAND, nil), nil
	case parserModel.GET_COMMAND:
		resp, err := slaveParser.processGetCommand(strCommand)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.GET_COMMAND, nil), nil
	case parserModel.REPLCONF:
		resp, err := slaveParser.processReplconfCommand(strCommand)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.GETACK, nil), nil
	default:
		return parserModel.CommandOutput{}, errors.New("unknown command")
	}
}

func (slaveParser *SlaveParser) processReplconfCommand(strCommand []string) (string, error) {
	switch strings.ToLower(strCommand[1]) {
	case parserModel.GETACK:
		encodeArray := []string{parserModel.REPLCONF, parserModel.ACK_RESP, strconv.Itoa(int(storage.GetRedisStorageInsight().Get()))}
		respData := encodeArrayString(encodeArray)
		return respData, nil
	default:
		return "", errors.New("invalid format for REPLCONF command")
	}
}

func (slaveParser *SlaveParser) processInfoCommand(strCommand []string) (string, error) {
	if len(strCommand) > 1 && strings.ToLower(strCommand[1]) == parserModel.INFO_REPLICATION {
		return encodeBulkString(fmt.Sprintf(parserModel.REPLICATION, config.GetRedisServerConfig().GetServerType())), nil
	}
	return "", errors.New("invalid format for INFO command")
}

func (slaveParser *SlaveParser) processSetCommand(strCommand []string, numElements int) (string, error) {
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

func (slaveParser *SlaveParser) processGetCommand(strCommand []string) (string, error) {
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

	return encodeBulkString(fmt.Sprint(value)), nil
}
