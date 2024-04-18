package commands

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

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
		fmt.Printf("Command not found: %s\n", strCommand)
		return encodeSimpleString("PONG")
	}

	if err != nil {
		log.Println("ERROR: ", err.Error())
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
		log.Printf("ERROR: %s\n", err.Error())
		return false
	}
	defer conn.Close()

	// Send PING command
	_, err = conn.Write([]byte(encodeArrayString([]string{parserModel.PING_COMMAND})))
	if err != nil {
		log.Printf("ERROR: Failed to write to connection: %s\n", err.Error())
		return false
	}

	// Read the response
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		log.Printf("ERROR: Failed to read from connection: %s\n", err.Error())
		return false
	}

	response := string(buffer[:n])
	expectedResponse := encodeSimpleString("PONG")

	if response != expectedResponse {
		log.Printf("ERROR: Invalid response from master server. Expected %s, got %s\n", expectedResponse, response)
		return false
	}

	return true
}
