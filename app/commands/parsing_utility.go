package commands

import (
	"log"
	"strconv"
	"strings"

	parserModel "github.com/codecrafters-io/redis-starter-go/app/models"
)

// splitByMultiple parses a string based on specific command formats and splits it into commands.
func splitByMultiple(input string) []string {
	// Replace escaped characters "\r\n" with a custom wrapper defined by parserModel
	input = strings.ReplaceAll(input, "\\r\\n", parserModel.STR_WRAPPER)

	// Split the input string using the custom wrapper as delimiter
	splittedStrs := strings.Split(input, parserModel.STR_WRAPPER)

	var resultCommands []string
	var index int
	lengthOfSplittedStrs := len(splittedStrs)

	for index < lengthOfSplittedStrs {
		current := splittedStrs[index]

		if len(current) == 0 {
			// Skip empty strings
			index++
			continue
		}

		firstChar := string(current[0])

		switch firstChar {
		case parserModel.SIMPLE:
			// Handle simple command (e.g., no action needed)
			index++
		case parserModel.BULK:
			// Handle bulk string format
			index++

			if index >= lengthOfSplittedStrs {
				break
			}

			next := splittedStrs[index]
			commandIndex := getIndexForCmd(next)

			if commandIndex != -1 {
				// Detected a different command within bulk string (e.g., RDB file)
				lengthOfSplittedStrs++
				index++
				nextCommand := next[commandIndex:]
				splittedStrs = append(splittedStrs[:index], append([]string{nextCommand}, splittedStrs[index:]...)...)
			} else {
				// Normal bulk string, add to result
				index++
			}
		case parserModel.ARRAYS:
			// Handle arrays format
			arrayString := current

			// Parse number of elements
			numElements, err := strconv.Atoi(current[1:])
			if err != nil {
				log.Printf("Error parsing number of elements: %v\n", err)
				return nil // Return nil to indicate error
			}

			arrayString += parserModel.STR_WRAPPER
			index++

			for i := 0; i < numElements; i++ {
				if index >= lengthOfSplittedStrs {
					break
				}

				// Add array element and its content to the arrayString
				arrayString += splittedStrs[index] + parserModel.STR_WRAPPER
				index++
				if index >= lengthOfSplittedStrs {
					break
				}
				arrayString += splittedStrs[index] + parserModel.STR_WRAPPER
				index++
			}

			resultCommands = append(resultCommands, arrayString)
		default:
			// Invalid command encountered
			log.Println("Invalid command detected.")
			return nil // Return nil to indicate error
		}
	}

	return resultCommands
}

// getIndexForCmd finds the index of a specific command type within a string.
func getIndexForCmd(s string) int {
	dataTypes := []string{parserModel.SIMPLE, parserModel.BULK, parserModel.ARRAYS}

	for _, dataType := range dataTypes {
		index := strings.Index(s, dataType)
		if index != -1 {
			return index
		}
	}

	return -1
}
