package commands

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	log "github.com/codecrafters-io/redis-starter-go/app/logger"
	parserModel "github.com/codecrafters-io/redis-starter-go/app/models"
	config "github.com/codecrafters-io/redis-starter-go/app/utility"
)

type Parser interface {
	ProcessArrayCommand(strCommand []string, numElements int) (string, error)
}

func HandleCommand(strCommand string) string {
	if len(strCommand) == 0 {
		return respError(errors.New("empty command"))
	}

	// Get the first character of the command
	startChar := string(strCommand[0])

	var resp string
	var err error
	var parserObj Parser

	// Determine whether it's a master or slave server
	if config.GetRedisServerConfig().GetServerType() == config.MASTER_SERVER {
		parserObj = &MasterParser{}
	} else {
		parserObj = &SlaveParser{}
	}

	// Split the command by STR_WRAPPER
	splittedCommand := strings.Split(strCommand, parserModel.STR_WRAPPER)

	switch startChar {
	case parserModel.ARRAYS:
		resp, err = processArrayCommand(parserObj, splittedCommand)
	default:
		log.LogError(errors.New("invalid command"))
		return encodeSimpleString("PONG")
	}

	if err != nil {
		log.LogError(err)
		return respError(err)
	}

	return resp
}

func processArrayCommand(parser Parser, splittedCommand []string) (string, error) {
	// Get the number of elements in the array
	numElements, err := strconv.Atoi(splittedCommand[0][1:])
	if err != nil || numElements == 0 {
		return "", errors.New("invalid format for array")
	}

	// Get the elements of the array
	var arrayElements []string
	startIndex := 2
	for i := 0; i < numElements; i++ {
		arrayElements = append(arrayElements, splittedCommand[startIndex])
		startIndex += 2
	}

	// Process the array command
	return parser.ProcessArrayCommand(arrayElements, numElements)
}

func CheckConnectionWithMaster() bool {
	// Get Replica Host and Port
	replicaHost := config.GetRedisServerConfig().GetReplicaHost()
	replicaPort := config.GetRedisServerConfig().GetReplicaPort()

	// Send a PING command to the master server to check the connection
	address := fmt.Sprintf("%s:%d", replicaHost, replicaPort)

	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.LogError(err)
		return false
	}

	defer conn.Close()

	requestCommands := []string{
		encodeArrayString([]string{parserModel.PING_COMMAND}),
		encodeArrayString([]string{parserModel.REPLCONF, parserModel.REPLCONF_LISTEN_PORT, fmt.Sprint(config.GetRedisServerConfig().GetPort())}),
		encodeArrayString([]string{parserModel.REPLCONF, parserModel.REPLCONF_CAPA, parserModel.REPLCONF_PYSYNC2}),
		encodeArrayString([]string{parserModel.PYSNC, "?", "-1"}),
	}

	expectedResponses := []string{
		encodeSimpleString("PONG"),
		encodeSimpleString("OK"),
		encodeSimpleString("OK"),
		encodeSimpleString("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"),
	}

	for i, command := range requestCommands {

		log.LogInfo(fmt.Sprintf("Sending command to master: %q\n", command))

		_, err := conn.Write([]byte(command))
		if err != nil {
			log.LogError(fmt.Errorf("error writing data: %s", err.Error()))
			return false
		}

		response := make([]byte, 1024)

		n, err := conn.Read(response)
		if err != nil {
			log.LogError(fmt.Errorf("error reading data: %s", err.Error()))
			return false
		}

		response = response[:n] // Trim the buffer to the actual number of bytes read

		if string(response) != expectedResponses[i] && i != 3 {
			log.LogError(fmt.Errorf("invalid response from master: %q", string(response)))
			return false
		} else {
			log.LogInfo(fmt.Sprintf("Received response from master: %q", string(response)))
		}
	}

	return true
}
