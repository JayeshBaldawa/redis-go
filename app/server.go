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

	defer func() {
		if r := recover(); r != nil {
			log.LogError(fmt.Errorf("panic occurred: %s", r))
			conn.Write([]byte(fmt.Sprintf("Error: %s", r)))
		}
	}()

	defer conn.Close()

	log.LogInfo(fmt.Sprintf("Connection received from %q", conn.RemoteAddr()))

	// Reading data in a loop
	buf := make([]byte, 1024)
	for {
		// Read timeout
		/*
			readTimeout := time.Duration(config.GetReadTimeout()) * time.Second
			err := conn.SetReadDeadline(time.Now().Add(readTimeout))
			if err != nil {
				log.LogError(fmt.Errorf("error setting read deadline: %s", err.Error()))
				return
			}
		*/

		n, err := conn.Read(buf)
		if err != nil {
			if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "wsarecv") || errors.Is(err, net.ErrClosed) {
				log.LogInfo(fmt.Sprintf("Connection closed by %q", conn.RemoteAddr()))
				if config.GetRedisServerConfig().IsMaster() {
					commands.RemoveReplicaServer(conn) // Remove the replica server from the list if available as sla
				}
				return
			}
			log.LogError(fmt.Errorf("error reading data: %s", err.Error()))
			return
		}

		// Trim trailing whitespace
		command := strings.TrimSpace(string(buf[:n]))
		// Replace \\ with \
		command = strings.ReplaceAll(command, "\\\\", "\\")

		if command == "exit" {
			log.LogInfo(fmt.Sprintf("Connection closed by %q", conn.RemoteAddr()))
			return
		}

		log.LogInfo(fmt.Sprintf("Received command: %q", command))

		// Handle the command
		commands.HandleCommand(command, conn)
	}
}

func readArgsPassed() {
	redisServerConfig := config.GetRedisServerConfig()
	args := os.Args[1:] // Skip the first argument which is the program name
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--port":
			i++
			redisServerConfig.SetPort(getPort(args[i]))
		case "--replicaof":
			i++
			redisServerConfig.SetServerType(config.SLAVE_SERVER)
			redisServerConfig.SetReplicaHost(args[i])
			i++
			redisServerConfig.SetReplicaPort(getPort(args[i]))
			log.LogInfo(fmt.Sprintf("Replicating data from %s:%d", redisServerConfig.GetReplicaHost(), redisServerConfig.GetReplicaPort()))
			var conn net.Conn
			var success bool
			// Check connection with master
			if success, conn = commands.CheckConnectionWithMaster(); !success {
				log.LogError(fmt.Errorf("failed to connect to master server"))
				os.Exit(1)
			}
			go handleRequest(conn) // Handle the connection with the master server
		}
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
