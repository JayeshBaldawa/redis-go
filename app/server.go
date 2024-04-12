package main

import (
	"fmt"
	"log"
	"net"
	"os"
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

	for {
		// Reading data
		buf := make([]byte, 1024)
		receivedData, err := conn.Read(buf)
		if err != nil {
			log.Printf("ERROR: Something went wrong while reading the data: %s", err.Error())
		}

		log.Printf("INFO: Received data - %d", receivedData)

		// Writing Back: Assuming ping request
		pongMsg := []byte("+PONG\r\n")
		n, err := conn.Write(pongMsg)
		if err != nil {
			log.Printf("ERROR: Something went wrong while writing the data: %s", err.Error())
		}
		log.Printf("INFO: Written data - %d", n)
	}
}
