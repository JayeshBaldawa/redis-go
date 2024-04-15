package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
	log.Println("INFO: Creating redis server at :6379")
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("ERROR: Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	log.Println("INFO: Listening to port 6379")
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

		// Trim trailing whitespace and convert to lowercase for case-insensitive comparison
		command := strings.TrimSpace(strings.ToLower(string(buf[:n])))

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
