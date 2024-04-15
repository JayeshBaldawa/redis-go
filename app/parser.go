package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
)

const (
	SIMPLE = "+"
	ERROR  = "-"
	BULK   = "$"
	ARRAYS = "*"
)

const (
	STR_WRAPPER = "\r\n"
	FIRST_BYTE  = "$"
)

const (
	ECHO_COMMAND = "echo"
	PING_COMMAND = "ping"
)

func encodeBulkString(resp string) string {
	return fmt.Sprintf(FIRST_BYTE+"%d"+STR_WRAPPER+"%s"+STR_WRAPPER, len(resp), resp)
}

func encodeSimpleString(resp string) string {
	return SIMPLE + resp + STR_WRAPPER
}

func respError(err error) string {
	return ERROR + " " + err.Error() + STR_WRAPPER
}

func processArrayCommand(strCommand []string, numElements int) (string, error) {

	switch strings.ToLower(strCommand[0]) {
	case ECHO_COMMAND:
		return encodeBulkString(strCommand[1]), nil
	case PING_COMMAND:
		return encodeSimpleString("PONG"), nil
	}

	return "", errors.New("command not found")
}

func handleCommand(strCommand string) string {

	if len(strCommand) == 0 {
		return respError(errors.New("empty command"))
	}

	// Get first byte of command
	startChar := string(strCommand[0])
	var resp string
	var err error

	// Split Command by STR_WRAPPER
	splittedCommand := strings.Split(strCommand, STR_WRAPPER)

	switch startChar {
	case ARRAYS:
		// Get the number of elements in the array
		var numElements int
		numElements, err = strconv.Atoi(splittedCommand[0][1:])
		if err != nil {
			err = errors.New("not valid format for array")
			break
		}

		if numElements == 0 || len(splittedCommand) < numElements*2 {
			err = errors.New("not valid format for array")
			break
		}

		// Get the elements of the array
		var arrayElements []string
		startIndex := 2
		for i := 0; i < numElements; i++ {
			arrayElements = append(arrayElements, splittedCommand[startIndex])
			startIndex = startIndex + 2
		}

		resp, err = processArrayCommand(arrayElements, numElements)
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
