package commands

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/codecrafters-io/redis-starter-go/app/logger"
	parserModel "github.com/codecrafters-io/redis-starter-go/app/models"
	storageModel "github.com/codecrafters-io/redis-starter-go/app/storage"
	config "github.com/codecrafters-io/redis-starter-go/app/utility"
)

type Parser interface {
	ProcessArrayCommand(strCommand []string, numElements int) (parserModel.CommandOutput, error)
}

var replicaServers sync.Map
var replicaServersCount int

// List of keywords indicating a slave command for the connection
var slaveKeywords = []string{parserModel.PYSNC}

// List of write back commands for the CDN
var writeBackCommands = []string{parserModel.SET_COMMAND}

// List of write back commands for the Slave
var slaveRespondCommand = []string{parserModel.SET_COMMAND, parserModel.PING_COMMAND, parserModel.ECHO_COMMAND}

func HandleCommand(strCommand string, conn net.Conn) (isSlaveReq bool) {

	isSlaveReq = false

	if len(strCommand) == 0 {
		resp := parserModel.CommandOutput{
			CommandName: "",
			Response:    encodeErrorString(errors.New("no command provided")),
		}
		WriteBackToConnection(conn, resp)
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
				CommandName: "",
				Response:    encodeErrorString(err),
			}
		}

		if err != nil {
			log.LogInfo(err.Error())
			resp = parserModel.CommandOutput{
				CommandName: "",
				Response:    encodeErrorString(err),
			}
			WriteBackToConnection(conn, resp)
			continue // Skip the rest of the loop
		}

		if isSlaveConnectionRequest(resp.CommandName) {
			log.LogInfo(fmt.Sprintf("Slave connection request: %q", resp.CommandName))
			// Add the connection to the list of replica servers
			replicaServers.Store(conn, true)
			replicaServersCount++
			isSlaveReq = true // So that for loop can break
		}

		// Write the response to all replica servers if the server is a master server
		if config.GetRedisServerConfig().IsMaster() && shouldReplicate(resp.CommandName) {

			go writeBackToReplicaServers(strCommand)
		}

		if shouldWriteBack(resp.CommandName) {
			WriteBackToConnection(conn, resp)
		}

		// Increment the processed bytes
		storageModel.GetRedisStorageInsight().Set(int64(len(cmd)))
		// Add the command to the stack
		storageModel.GetStackCmdStruct().AddCommand(resp.CommandName)
	}

	return
}

func writeBackToReplicaServers(data string) {
	replicaServers.Range(func(key, value interface{}) bool {
		conn := key.(net.Conn)
		_, err := conn.Write([]byte(data + "\r\n"))
		log.LogInfo(fmt.Sprintf("Writing data to replica server %q", conn.RemoteAddr()))
		if err != nil {
			log.LogError(fmt.Errorf("error writing data to replica server: %s", err.Error()))
			// Remove the replica server from the list if available as slave
			RemoveReplicaServer(conn)
			replicaServersCount--
		}
		return true
	})
}

func RemoveReplicaServer(replicaServer net.Conn) {
	replicaServers.Delete(replicaServer)
	replicaServersCount--
	log.LogInfo(fmt.Sprintf("Replica server %q removed", replicaServer.RemoteAddr()))
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

func WriteBackToConnection(conn net.Conn, output parserModel.CommandOutput) {
	// Get the command and response
	cmd := output.CommandName
	data := output.Response

	if shouldSync(cmd) {
		ackRc, err := waitForReplicationSync(output)
		log.LogInfo(fmt.Sprintf("Final ACK received: %d", ackRc))
		if err != nil {
			log.LogError(fmt.Errorf("error waiting for replication sync: %s", err.Error()))
			data = encodeErrorString(err)
			cmd = ""
		} else {
			data = encodeIntegerString(ackRc)
		}

	}

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

func shouldSync(cmdName string) bool {
	switch cmdName {
	case parserModel.WAIT:
		return true
	}
	return false

}

func waitForReplicationSync(input parserModel.CommandOutput) (int, error) {

	if !config.GetRedisServerConfig().IsMaster() || replicaServersCount == 0 {
		return 0, nil // No replication needed if not a master or no replica servers
	}

	if input.CommandName != parserModel.WAIT {
		return -1, fmt.Errorf("unexpected command: %s", input.CommandName)
	}

	if storageModel.GetStackCmdStruct().GetTopOfStack() != parserModel.SET_COMMAND {
		return replicaServersCount, nil
	}

	numOfAckNeededStr := input.Parameters[parserModel.WAIT_REPLICAS_COUNT]
	if numOfAckNeededStr == "0" {
		return replicaServersCount, nil
	}

	_, err := strconv.Atoi(numOfAckNeededStr)
	if err != nil {
		return -1, fmt.Errorf("error converting number of replicas needed to integer: %w", err)
	}

	msString := input.Parameters[parserModel.WAIT_TIMEOUT]
	milliseconds, err := strconv.ParseInt(msString, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("error converting milliseconds to integer: %w", err)
	}

	if milliseconds == 0 {
		// Set default timeout to 5 seconds if not provided
		milliseconds = 5000
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(milliseconds)*time.Millisecond)
	defer cancel() // Ensure cancellation of the context when function exits

	// Use atomic integer for acknowledgment counter
	// var ackCounter int32
	var countSpawn int

	var testChan = make(chan int, replicaServersCount)

	replicaServers.Range(func(key, value interface{}) bool {
		conn := key.(net.Conn)
		countSpawn++
		go func(conn net.Conn) {
			err := sendAcknowledgementsForSync(milliseconds, conn, ctx)
			fmt.Println("DID I GET HERE")
			if err != nil {
				log.LogError(fmt.Errorf("error sending ACK request to replica server: %s", err.Error()))
				testChan <- 0
				return
			}
			testChan <- 1
		}(conn)
		return true
	})

	var test int

	// Wait for the context to be done
	for i := 0; i < countSpawn; i++ {
		val := <-testChan
		if val == 1 {
			test++
		}
	}

	return test, nil
}

func sendAcknowledgementsForSync(duration int64, Relpconn net.Conn, ctx context.Context) error {

	defer func() {
		if r := recover(); r != nil {
			log.LogError(fmt.Errorf("recovered from panic: %v", r))
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err() // Return context error on cancellation
	default:
		// Send ACK request to the replica server
		ackArray := []string{strings.ToUpper(parserModel.REPLCONF), strings.ToUpper(parserModel.GETACK), "*"}
		encodedRequest := encodeArrayString(ackArray)

		if _, err := Relpconn.Write([]byte(encodedRequest)); err != nil {
			log.LogError(fmt.Errorf("error writing ACK request to replica server %s: %v", Relpconn.RemoteAddr(), err))
			return err
		}

		log.LogInfo(fmt.Sprintf("ACK request sent to replica server %q", Relpconn.RemoteAddr()))

		// Add a read timeout to the connection using the context
		if err := Relpconn.SetReadDeadline(time.Now().Add(time.Duration(duration) * time.Millisecond)); err != nil {
			log.LogError(fmt.Errorf("error setting read deadline on replica server %s: %v", Relpconn.RemoteAddr(), err))
			return err
		}

		// Read the response from the replica server
		respReceived := make([]byte, 1024)
		n, err := Relpconn.Read(respReceived)
		if err != nil {
			if err != io.EOF {
				log.LogError(fmt.Errorf("error reading ACK response from replica server %s: %v", Relpconn.RemoteAddr(), err))
			}
			return err
		}

		resp := respReceived[:n] // Trim the buffer to the actual number of bytes read

		log.LogInfo(fmt.Sprintf("ACK response received from replica server %q: %q", Relpconn.RemoteAddr(), resp))

		return nil
	}
}
