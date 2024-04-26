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

func (masterParser *MasterParser) ProcessArrayCommand(strCommand []string, numElements int) (parserModel.CommandOutput, error) {
	// Ensure at least one command is provided
	if len(strCommand) < 1 {
		return parserModel.CommandOutput{}, errors.New("no command provided")
	}

	// Convert command to lower case for case-insensitive comparison
	command := strings.ToLower(strCommand[0])
	switch command {
	case parserModel.ECHO_COMMAND:
		return formatCommandOutput(encodeBulkString(getCommandParameter(strCommand, 1)), parserModel.ECHO_COMMAND), nil
	case parserModel.PING_COMMAND:
		return formatCommandOutput(encodeSimpleString("PONG"), parserModel.PING_COMMAND), nil
	case parserModel.SET_COMMAND:
		resp, err := masterParser.processSetCommand(strCommand, numElements)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.SET_COMMAND), nil
	case parserModel.GET_COMMAND:
		resp, err := masterParser.processGetCommand(strCommand)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.GET_COMMAND), nil
	case parserModel.INFO_COMMAND:
		resp, err := masterParser.processInfoCommand(strCommand)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.INFO_COMMAND), nil
	case parserModel.REPLCONF:
		resp, err := masterParser.checkReplconCommand(strCommand)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.REPLCONF), nil
	case parserModel.PYSNC:
		return formatCommandOutput(masterParser.handlePysncCommand(), parserModel.PYSNC), nil
	case parserModel.WAIT:
		_, err := strconv.Atoi(strCommand[1])
		if err != nil {
			return parserModel.CommandOutput{}, errors.New("invalid format for WAIT command")
		}
		return formatCommandOutput(encodeIntegerString(replicaServersCount), parserModel.WAIT), nil
	default:
		return parserModel.CommandOutput{}, errors.New("unknown command")
	}
}

func (masterParser *MasterParser) handlePysncCommand() string {
	return encodeSimpleString(parserModel.FULLRESYNC + " 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0")
}

func (masterParser *MasterParser) checkReplconCommand(strCommand []string) (string, error) {
	switch strings.ToLower(strCommand[1]) {
	case parserModel.REPLCONF_LISTEN_PORT:
		return encodeSimpleString("OK"), nil
	case parserModel.REPLCONF_CAPA:
		if strings.ToLower(strCommand[2]) == parserModel.REPLCONF_PYSYNC2 {
			return encodeSimpleString("OK"), nil
		}
	case parserModel.GETACK:
		return encodeBulkString(parserModel.REPLCONF + " " + parserModel.ACK_RESP + " 0"), nil
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
