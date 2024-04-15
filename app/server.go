package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

type RedisServer struct {
	port        int
	replicaHost string
	replicaPort int
	serverType  string
}

var redisServerConfig *RedisServer

func init() {
	redisServerConfig = &RedisServer{
		port:        6379,
		replicaHost: "",
		replicaPort: 0,
		serverType:  "master",
	}
}

func main() {

	readFlagsPassed()

	log.Printf("INFO: Creating redis server at : %d", redisServerConfig.port)

	l, err := net.Listen("tcp", "0.0.0.0:"+fmt.Sprintf("%d", redisServerConfig.port))
	if err != nil {
		fmt.Printf("ERROR: Failed to bind to port + %d", redisServerConfig.port)
		os.Exit(1)
	}
	defer l.Close()

	log.Printf("INFO: Listening to port %d", redisServerConfig.port)

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
		resp := handleCommand(command)

		// Write the response back to the client
		_, err = conn.Write([]byte(resp))
		if err != nil {
			log.Printf("ERROR: Failed to write response to connection: %s", err.Error())
			return
		}
	}
}

func readFlagsPassed() {
	port := flag.Int("port", redisServerConfig.port, "Port to run the server on")
	replicaHost := flag.String("replicaof", "", "Host to replicate to")
	flag.Parse()
	if *replicaHost != "" {
		redisServerConfig.serverType = "slave"
	}
	redisServerConfig.port = *port
	redisServerConfig.replicaHost = *replicaHost
	fmt.Println("Port: ", *port)
}
