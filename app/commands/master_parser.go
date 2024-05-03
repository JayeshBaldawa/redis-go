package commands

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/events"
	log "github.com/codecrafters-io/redis-starter-go/app/logger"
	parserModel "github.com/codecrafters-io/redis-starter-go/app/models"
	storage "github.com/codecrafters-io/redis-starter-go/app/storage"
	config "github.com/codecrafters-io/redis-starter-go/app/utility"
	"github.com/google/uuid"
)

type MasterParser struct{}

func (masterParser *MasterParser) ProcessArrayCommand(input parserModel.CommandInput, numElements int) (parserModel.CommandOutput, error) {

	strCommand := input.SplittedCommand

	// Ensure at least one command is provided
	if len(strCommand) < 1 {
		return parserModel.CommandOutput{}, errors.New("no command provided")
	}

	// Convert command to lower case for case-insensitive comparison
	command := strings.ToLower(strCommand[0])
	switch command {
	case parserModel.ECHO_COMMAND:
		return formatCommandOutput(encodeBulkString(getCommandParameter(strCommand, 1)), parserModel.ECHO_COMMAND, nil, false), nil

	case parserModel.PING_COMMAND:
		return formatCommandOutput(encodeSimpleString("PONG"), parserModel.PING_COMMAND, nil, false), nil

	case parserModel.SET_COMMAND:
		resp, err := masterParser.processSetCommand(strCommand, numElements)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.SET_COMMAND, nil, false), nil

	case parserModel.GET_COMMAND:
		resp, err := masterParser.processGetCommand(strCommand)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.GET_COMMAND, nil, false), nil

	case parserModel.INFO_COMMAND:
		resp, err := masterParser.processInfoCommand(strCommand)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.INFO_COMMAND, nil, false), nil

	case parserModel.REPLCONF:
		resp, err := masterParser.checkReplconCommand(strCommand)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.REPLCONF, nil, false), nil

	case parserModel.PYSNC:
		return formatCommandOutput(masterParser.handlePysncCommand(), parserModel.PYSNC, nil, false), nil

	case parserModel.WAIT:
		replicaServersCount, err := strconv.Atoi(strCommand[1])
		if err != nil {
			return parserModel.CommandOutput{}, errors.New("invalid format for WAIT command")
		}

		timeOut, err := strconv.Atoi(strCommand[2])
		if err != nil {
			return parserModel.CommandOutput{}, errors.New("invalid format for WAIT command")
		}

		mapReplicaServers := map[string]string{
			parserModel.WAIT_TIMEOUT:        fmt.Sprint(timeOut),
			parserModel.WAIT_REPLICAS_COUNT: fmt.Sprint(replicaServersCount),
		}

		return formatCommandOutput(encodeIntegerString(replicaServersCount), parserModel.WAIT, mapReplicaServers, false), nil

	case parserModel.TYPE_COMMAND:
		typeOfValue, err := processTypeCommand(strCommand[1])
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(typeOfValue, parserModel.TYPE_COMMAND, nil, false), nil
	case parserModel.XADD_COMMAND:
		resp, err := masterParser.processSetStream(strCommand, numElements)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.XADD_COMMAND, nil, false), nil

	case parserModel.XRANGE_COMMAND:
		resp, err := masterParser.processXRangeCommand(strCommand)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		return formatCommandOutput(resp, parserModel.XRANGE_COMMAND, nil, false), nil

	case parserModel.XREAD_COMMAND:
		resp, isStreaming, err := masterParser.processXReadCommand(strCommand, input.Conn)
		if err != nil {
			return parserModel.CommandOutput{}, err
		}
		if isStreaming {
			return formatCommandOutput(resp, parserModel.XREAD_COMMAND, map[string]string{
				parserModel.XREAD_TOPIC: uuid.New().String(),
			}, isStreaming), nil
		}
		return formatCommandOutput(resp, parserModel.XREAD_COMMAND, nil, isStreaming), nil

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

	return encodeBulkString(fmt.Sprint(value)), nil
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

func processTypeCommand(key string) (string, error) {
	value, err := storage.GetStorage().Get(key)
	if err != nil {
		return "", err
	}

	if value == "" {
		// Check if key exists in stream storage
		stream := storage.GetStreamStorage().GetStream(key)
		if len(stream) > 0 {
			return encodeSimpleString("stream"), nil
		}
		return encodeNoneTypeString(), nil
	}

	switch value.(type) {
	case string:
		return encodeSimpleString("string"), nil
	case int:
		return encodeSimpleString("integer"), nil
	default:
		return encodeSimpleString("none"), nil
	}
}

func (masterParser *MasterParser) processSetStream(strCommand []string, numElements int) (string, error) {
	if numElements < 3 {
		return "", errors.New("invalid format for XADD command")
	}

	keyForStream := strCommand[1] // Stream key
	entryId := strCommand[2]      // Entry id

	attributes := make(map[string]interface{})

	for i := 3; i < numElements; i += 2 {
		attributes[strCommand[i]] = strCommand[i+1]
	}

	entryId, err := storage.GetStreamStorage().AddEntry(entryId, attributes, keyForStream)

	if err != nil {
		return "", err
	}

	return encodeBulkString(entryId), nil
}

func (masterParser *MasterParser) processXRangeCommand(strCommand []string) (string, error) {
	if len(strCommand) < 4 {
		return "", errors.New("invalid format for XRANGE command")
	}

	key := strCommand[1]
	start := strCommand[2]
	end := strCommand[3]

	entries := storage.GetStreamStorage().GetRange(key, start, end)
	if len(entries) == 0 {
		return encodeNullBulkString(), nil
	}

	return encodeStreamArrayString(entries), nil
}

func (masterParser *MasterParser) processXReadCommand(strCommand []string, conn net.Conn) (string, bool, error) {

	if len(strCommand) < 4 {
		return "", false, errors.New("invalid format for XREAD command")
	}

	// Check if there are any optional argument
	switch strings.ToLower(strCommand[1]) {
	case parserModel.XREAD_COMMAND_BLOCK:
		return handleXreadBlockCommand(strCommand, conn)
	}

	streams := make(map[string]string)
	numToRun := (len(strCommand) - 2) / 2

	orderOfKeys := make([]string, 0)

	for i := 0; i < numToRun; i++ {
		streams[strCommand[i+2]] = strCommand[i+2+numToRun]
		orderOfKeys = append(orderOfKeys, strCommand[i+2])
	}

	entries := storage.GetStreamStorage().XReadStreams(streams)
	if len(entries) == 0 {
		return encodeNullBulkString(), false, nil
	}

	return encodeXreadStreamArrayString(entries, orderOfKeys), false, nil
}

func handleXreadBlockCommand(strCommand []string, conn net.Conn) (string, bool, error) {
	//xread block 1000 streams some_key 1526985054069-0
	if len(strCommand) < 5 {
		return encodeErrorString(errors.New("invalid format for XREAD BLOCK command")), false, nil
	}

	// Get the timeout value
	timeout, err := strconv.Atoi(strCommand[2])
	if err != nil {
		return encodeErrorString(errors.New("invalid format for timeout value")), false, nil
	}

	// Check if the timeout value is valid
	if timeout < 0 {
		return encodeErrorString(errors.New("timeout value should be greater than 0")), false, nil
	}

	// GetStreamKeyName
	streamKey := strCommand[4]
	entryId := strCommand[5]

	if timeout == 0 {
		go StreamXReadBlock(streamKey, entryId, timeout, conn)
		return "", true, nil
	}

	entries, err := storage.GetStreamStorage().XReadStreamsBlock(streamKey, entryId, timeout)

	if err != nil {
		return encodeErrorString(err), false, nil
	}

	if len(entries) == 0 {
		return encodeNullBulkString(), false, nil
	}

	return encodeXreadStreamArrayString(entries, []string{streamKey}), false, nil
}

func StreamXReadBlock(streamKey string, entryId string, timeout int, conn net.Conn) {

	topic := fmt.Sprintf("%s:%s:%s", parserModel.XREAD_STREAM_TOPIC, streamKey, uuid.New().String())

	log.LogInfo(fmt.Sprintf("Subscribing to topic: %s", topic))

	subChan := events.GetPubSub().Subscribe(topic)
	defer events.GetPubSub().Unsubscribe(topic, subChan)

	defaultTimeoutMs := 1000000 // 16666.6667

	timeout = defaultTimeoutMs

	go storage.GetStreamStorage().XReadStreamsBlock(streamKey, entryId, timeout, topic)

	for {
		select {
		case <-time.After(time.Duration(defaultTimeoutMs) * time.Millisecond):
			return
		case data := <-subChan:
			log.LogInfo(fmt.Sprintf("Received data from topic: %v", data))
			entries, ok := data.Data.(storage.StreamEntry)
			if !ok {
				// Check to stop the stream
				stopCmd, ok := data.Data.(string)
				if ok && stopCmd == "STOP" {
					return
				}
			}

			// Write the data to the connection
			conn.Write([]byte(encodeXreadStreamArrayString(map[string][]storage.StreamEntry{streamKey: {entries}}, []string{streamKey})))
		}
	}

}
