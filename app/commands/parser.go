package commands

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	log "github.com/codecrafters-io/redis-starter-go/app/logger"
	parserModel "github.com/codecrafters-io/redis-starter-go/app/models"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
	config "github.com/codecrafters-io/redis-starter-go/app/utility"
)

type Parser interface {
	ProcessArrayCommand(strCommand []string, numElements int) (parserModel.CommandOutput, error)
}

// List of replica servers
var replicaServers = []net.Conn{}

// List of keywords indicating a slave command for the connection
var slaveKeywords = []string{parserModel.PYSNC}

// List of write back commands for the CDN
var writeBackCommands = []string{parserModel.SET_COMMAND}

// List of write back commands for the Slave
var slaveRespondCommand = []string{parserModel.SET_COMMAND, parserModel.PING_COMMAND, parserModel.ECHO_COMMAND}

func HandleCommand(strCommand string, conn net.Conn) {

	if len(strCommand) == 0 {
		WriteBackToConnection(conn, respError(errors.New("no command provided")), "")
		return
	}

	var resp parserModel.CommandOutput
	var err error
	var parserObj Parser

	// Determine whether it's a master or slave server
	if config.GetRedisServerConfig().GetServerType() == config.MASTER_SERVER {
		parserObj = &MasterParser{}
	} else {
		parserObj = &SlaveParser{}
	}

	splittedCmds := splitCommands(strCommand)

	for _, cmd := range splittedCmds {

		// Get the first character of the command
		startChar := string(cmd[0])

		// Split the command by STR_WRAPPER
		splittedStrs := strings.Split(cmd, parserModel.STR_WRAPPER)

		switch startChar {
		case parserModel.ARRAYS:
			resp, err = processArrayCommand(parserObj, splittedStrs)
		default:
			err = errors.New("invalid command")
			log.LogInfo(err.Error())
			resp = parserModel.CommandOutput{
				ComamndName: "",
				Response:    respError(err),
			}
		}

		if err != nil {
			log.LogInfo(err.Error())
			WriteBackToConnection(conn, respError(err), "")
			continue // Skip the rest of the loop
		}

		if isSlaveConnectionRequest(resp.ComamndName) {
			log.LogInfo(fmt.Sprintf("Slave connection request: %q", resp.ComamndName))
			replicaServers = append(replicaServers, conn)
		}

		if !config.GetRedisServerConfig().IsMaster() {
			SetProcessedBytes(int64(len(cmd)))
		}

		if shouldWriteBack(resp.ComamndName) {
			WriteBackToConnection(conn, resp.Response, resp.ComamndName)
		}

		// Write the response to all replica servers if the server is a master server
		if config.GetRedisServerConfig().IsMaster() && shouldReplicate(resp.ComamndName) {
			go writeBackToReplicaServers(strCommand)
		}
	}
}

func writeBackToReplicaServers(data string) {
	// Write data to each replica server
	for _, replicaServer := range replicaServers {
		_, err := replicaServer.Write([]byte(data + "\r\n"))
		log.LogInfo(fmt.Sprintf("Writing data to replica server %q", replicaServer.RemoteAddr()))
		if err != nil {
			log.LogError(fmt.Errorf("error writing data to replica server: %s", err.Error()))
			// Remove the replica server from the list if there is an error
			replicaServers = removeReplicaServer(replicaServers, replicaServer)
			log.LogInfo(fmt.Sprintf("Replica server %q removed from the list", replicaServer.RemoteAddr()))
		}
	}
}

func removeReplicaServer(replicaServers []net.Conn, replicaServer net.Conn) []net.Conn {
	var newReplicaServers []net.Conn
	for _, server := range replicaServers {
		if server != replicaServer {
			newReplicaServers = append(newReplicaServers, server)
		}
	}
	return newReplicaServers
}

func shouldWriteBack(cmdName string) bool {
	if config.GetRedisServerConfig().IsMaster() {
		return true
	}
	// Check if the command contains any of the write back keywords
	for _, cmd := range slaveRespondCommand {
		if strings.Contains(cmdName, cmd) {
			return false
		}
	}
	return true
}

func shouldReplicate(receivedCmd string) bool {
	// Check if the command contains any of the replica keywords
	for _, cmd := range writeBackCommands {
		if strings.Contains(receivedCmd, cmd) {
			return true
		}
	}
	return false
}

func resendDataToConn(cmd string) (bool, string) {
	switch cmd {
	case parserModel.PYSNC:
		return true, encodeRDBResp()
	}
	return false, ""
}

func isSlaveConnectionRequest(cmd string) bool {
	// Check if the command contains any of the slave keywords
	for _, keyword := range slaveKeywords {
		if strings.Contains(cmd, keyword) {
			return true
		}
	}
	return false
}

func processArrayCommand(parser Parser, splittedCommand []string) (parserModel.CommandOutput, error) {
	// Get the number of elements in the array
	numElements, err := strconv.Atoi(splittedCommand[0][1:])
	if err != nil || numElements == 0 {
		return parserModel.CommandOutput{}, errors.New("invalid format for array")
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

func CheckConnectionWithMaster() (bool, net.Conn) {
	// Get Replica Host and Port
	replicaHost := config.GetRedisServerConfig().GetReplicaHost()
	replicaPort := config.GetRedisServerConfig().GetReplicaPort()

	// Send a PING command to the master server to check the connection
	address := fmt.Sprintf("%s:%d", replicaHost, replicaPort)

	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.LogError(err)
		return false, nil
	}

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
			return false, nil
		}

		bufferSize := 1024
		response := make([]byte, bufferSize)

		n, err := conn.Read(response)
		if err != nil {
			log.LogError(fmt.Errorf("error reading data: %s", err.Error()))
			return false, nil
		}

		response = response[:n] // Trim the buffer to the actual number of bytes read

		if i == 3 {
			// Send the response to be handled
			go HandleCommand(string(response), conn)
		}

		if string(response) != expectedResponses[i] && i != 3 {
			log.LogError(fmt.Errorf("invalid response from master: %q", string(response)))
			return false, nil
		} else {
			log.LogInfo(fmt.Sprintf("Received response from master: %q", string(response)))
		}
	}

	/*
		// Need to comment this because Codecrafters is sending RDB file along with the response of PYSNC command

			// Read the RDB file from the master server
			rdbData := make([]byte, bufferSize)
			n, err := conn.Read(rdbData)
			if err != nil {
				log.LogError(fmt.Errorf("error reading data: %s", err.Error()))
				return false, nil
			}

			rdbData = rdbData[:n] // Trim the buffer to the actual number of bytes read

	*/
	return true, conn
}

func WriteBackToConnection(conn net.Conn, data string, cmd string) {
	log.LogInfo(fmt.Sprintf("Command is %q --> Response is %q", cmd, data))
	// Send Response Back to Connection
	_, err := conn.Write([]byte(data))
	if err != nil {
		log.LogError(fmt.Errorf("error writing data: %s", err.Error()))
		return
	}

	if ok, resp := resendDataToConn(cmd); ok {
		_, err := conn.Write([]byte(resp))
		if err != nil {
			log.LogError(fmt.Errorf("error writing data: %s", err.Error()))
		}
	}
}

func splitCommands(strCommand string) []string {
	result := splitByMultiple(strCommand)
	log.LogInfo(fmt.Sprintf("Splitted Commands: %v", result))
	return result
}

func SetProcessedBytes(processedBytes int64) {
	storage.GetRedisStorageInsight().SetProcessedBytes(processedBytes)
}
