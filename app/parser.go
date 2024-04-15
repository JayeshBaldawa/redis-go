package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	storage "github.com/codecrafters-io/redis-starter-go/app/storage"
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
	REPLICATION = "# Replication\nrole:%s"
)

const (
	ECHO_COMMAND = "echo"
	PING_COMMAND = "ping"
	SET_COMMAND  = "set"
	GET_COMMAND  = "get"
	INFO_COMMAND = "info"
)

const (
	EX   = "ex"   // Seconds
	PX   = "px"   // Milliseconds
	EXAT = "exat" // Unix timestamp in seconds
	PXAT = "pxat" // Unix timestamp in milliseconds
)

func encodeBulkString(resp string) string {
	return fmt.Sprintf(FIRST_BYTE+"%d"+STR_WRAPPER+"%s"+STR_WRAPPER, len(resp), resp)
}

func encodeNullBulkString() string {
	return "$-1" + STR_WRAPPER
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
	case SET_COMMAND:
		if numElements != 5 && numElements != 3 {
			return "", errors.New("not valid format for set command")
		}

		var timeOfExpiry time.Time

		if numElements == 5 {
			// Get the expire time and type
			expire, err := strconv.Atoi(strCommand[4])
			if err != nil {
				return "", errors.New("not valid format for given time")
			}
			// Get the time in seconds
			timeOfExpiry = getExpiryTimeInUTC(expire, strCommand[3])
			// Check if the time is empty
			if timeOfExpiry.IsZero() {
				return "", errors.New("not valid format for time type")
			}
		}

		err := storage.GetStorage().Set(strCommand[1], strCommand[2], timeOfExpiry)
		if err != nil {
			return "", err
		}

		return encodeSimpleString("OK"), nil
	case GET_COMMAND:
		if len(strCommand) < 2 {
			return "", errors.New("not valid format for get command")
		}
		value, err := storage.GetStorage().Get(strCommand[1])
		if err != nil {
			return "", err
		}
		if value == "" {
			return encodeNullBulkString(), nil
		}
		return encodeBulkString(value), nil
	case INFO_COMMAND:
		return encodeBulkString(fmt.Sprintf(REPLICATION, redisServerConfig.serverType)), nil
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

func getExpiryTimeInUTC(expire int, Timetype string) time.Time {
	switch strings.ToLower(Timetype) {
	case EX:
		return time.Now().UTC().Add(time.Duration(expire) * time.Second)
	case PX:
		return time.Now().UTC().Add(time.Duration(expire) * time.Millisecond)
	case EXAT:
		return time.Unix(int64(expire), 0).UTC()
	case PXAT:
		return time.Unix(0, int64(expire)*int64(time.Millisecond)).UTC()
	default:
		return time.Time{}
	}
}
