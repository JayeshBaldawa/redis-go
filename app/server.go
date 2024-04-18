package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	commands "github.com/codecrafters-io/redis-starter-go/app/commands"
	config "github.com/codecrafters-io/redis-starter-go/app/utility"
)

func main() {

	readArgsPassed()

	port := config.GetRedisServerConfig().GetPort()

	log.Printf("INFO: Creating redis server at : %d", port)

	l, err := net.Listen("tcp", "0.0.0.0:"+fmt.Sprintf("%d", port))
	if err != nil {
		fmt.Printf("ERROR: Failed to bind to port + %d", port)
		os.Exit(1)
	}
	defer l.Close()

	log.Printf("INFO: Listening to port %d", port)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("ERROR: Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		// Handling the received request
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()

	log.Printf("INFO: Accepted connection from %s", conn.RemoteAddr())

	// Reading data in a loop
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("INFO: Client %s closed the connection", conn.RemoteAddr())
				return
			}
			log.Printf("ERROR: Failed to read from connection: %s", err.Error())
			return
		}

		// Trim trailing whitespace
		command := strings.TrimSpace(string(buf[:n]))

		if command == "exit" {
			log.Printf("INFO: Client %s requested to exit", conn.RemoteAddr())
			return
		}

		log.Printf("INFO: Received command from %s: %s", conn.RemoteAddr(), command)

		// Handle the command
		resp := commands.HandleCommand(command)

		// Write the response back to the client
		_, err = conn.Write([]byte(resp))
		if err != nil {
			log.Printf("ERROR: Failed to write response to connection: %s", err.Error())
			return
		}
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
			fmt.Printf("INFO: Replicating to %s:%d\n", redisServerConfig.GetReplicaHost(), redisServerConfig.GetReplicaPort())
			if !commands.CheckConnectionWithMaster() {
				log.Println("ERROR: Unable to connect to master server")
				os.Exit(1)
			}
		}
	}
}

func getPort(port string) int {
	portInt, err := strconv.Atoi(port)
	if err != nil {
		log.Println("ERROR: Invalid port number")
		os.Exit(1)
	}
	return portInt
}
