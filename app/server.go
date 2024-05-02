package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	commands "github.com/codecrafters-io/redis-starter-go/app/commands"
	log "github.com/codecrafters-io/redis-starter-go/app/logger"
	config "github.com/codecrafters-io/redis-starter-go/app/utility"
)

func main() {

	readArgsPassed()

	port := config.GetRedisServerConfig().GetPort()

	log.LogInfo(fmt.Sprintf("Starting server on port %d", port))

	l, err := net.Listen("tcp", "0.0.0.0:"+fmt.Sprintf("%d", port))
	if err != nil {
		log.LogError(fmt.Errorf("error starting server: %s", err.Error()))
		os.Exit(1)
	}
	defer l.Close()

	log.LogInfo("Server started successfully")

	for {
		conn, err := l.Accept()
		if err != nil {
			log.LogError(fmt.Errorf("error accepting connection: %s", err.Error()))
			os.Exit(1)
		}
		// Handling the received request
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {

	isSlaveReq := false

	defer func() {
		if r := recover(); r != nil {
			log.LogError(fmt.Errorf("panic occurred: %s", r))
			conn.Write([]byte(fmt.Sprintf("Error: %s", r)))
		}
		if !isSlaveReq {
			conn.Close() // Close the connection after handling the request
		}
	}()

	log.LogInfo(fmt.Sprintf("Connection received from %q", conn.RemoteAddr()))

	// Reading data in a loop
	buf := make([]byte, 1024)
	for {

		n, err := conn.Read(buf)
		if err != nil {
			if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "wsarecv") || errors.Is(err, net.ErrClosed) {
				log.LogInfo(fmt.Sprintf("Connection closed by %q", conn.RemoteAddr()))
				break
			}
			log.LogError(fmt.Errorf("error reading data: %s", err.Error()))
			break
		}

		// Trim trailing whitespace
		command := strings.TrimSpace(string(buf[:n]))
		// Replace \\ with \
		command = strings.ReplaceAll(command, "\\\\", "\\")

		if command == "exit" {
			log.LogInfo(fmt.Sprintf("Connection closed by %q", conn.RemoteAddr()))
			conn.Close()
			break
		}

		log.LogInfo(fmt.Sprintf("Received command: %q", command))

		// Handle the command
		if isSlaveReq = commands.HandleCommand(command, conn); isSlaveReq {
			break
		}
	}

}

func readArgsPassed() {
	// Get Redis server configuration from the application's configuration
	redisServerConfig := config.GetRedisServerConfig()

	// Extract command-line arguments, skipping the program name
	args := os.Args[1:]

	// Iterate through the arguments
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--port":
			// Increment i to move to the next argument, which should be the port number
			i++
			port := getPort(args[i])
			redisServerConfig.SetPort(port)

		case "--replicaof":
			// Increment i to move to the next argument, which should be the replica host
			i++
			// Set the server type to slave
			redisServerConfig.SetServerType(config.SLAVE_SERVER)
			replicaHost := args[i]
			redisServerConfig.SetReplicaHost(replicaHost)

			// Increment i again to move to the replica port argument
			i++
			replicaPort := getPort(args[i])
			redisServerConfig.SetReplicaPort(replicaPort)

			// Initialize logger and log the replication configuration
			log.InitLogger()
			log.LogInfo(fmt.Sprintf("Replicating data from %s:%d", replicaHost, replicaPort))

			// Check connection with the master server
			success, conn := commands.CheckConnectionWithMaster()
			if !success {
				log.LogError(fmt.Errorf("failed to connect to master server"))
				os.Exit(1)
			}

			// Handle the connection with the master server asynchronously
			go handleRequest(conn)
		}
	}

	// If the server type is master, initialize the logger
	if redisServerConfig.GetServerType() == config.MASTER_SERVER {
		log.InitLogger()
	}
}

func getPort(port string) int {
	portInt, err := strconv.Atoi(port)
	if err != nil {
		log.LogError(fmt.Errorf("invalid port number: %s", port))
		os.Exit(1)
	}
	return portInt
}
