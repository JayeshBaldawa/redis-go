package commands

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	parserModel "github.com/codecrafters-io/redis-starter-go/app/models"
)

func encodeBulkString(resp string) string {
	return fmt.Sprintf(parserModel.FIRST_BYTE+"%d"+parserModel.STR_WRAPPER+"%s"+parserModel.STR_WRAPPER, len(resp), resp)
}

func encodeNullBulkString() string {
	return "$-1" + parserModel.STR_WRAPPER
}

func encodeSimpleString(resp string) string {
	return parserModel.SIMPLE + resp + parserModel.STR_WRAPPER
}

func encodeArrayString(resps []string) string {
	bufferString := bytes.NewBufferString(parserModel.ARRAYS)
	bufferString.WriteString(strconv.Itoa(len(resps)))
	bufferString.WriteString(parserModel.STR_WRAPPER)
	for _, resp := range resps {
		bufferString.WriteString(encodeBulkString(resp))
	}
	return bufferString.String()
}

func encodeRDBResp() string {
	emptyRdb, _ := hex.DecodeString(parserModel.EMPTY_RDB_FILE)
	return strings.TrimRight(encodeBulkString(string(emptyRdb)), "\r\n")
}

func respError(err error) string {
	return parserModel.ERROR + " " + err.Error() + parserModel.STR_WRAPPER
}

func getExpiryTimeInUTC(expire int, Timetype string) time.Time {
	switch strings.ToLower(Timetype) {
	case parserModel.EX:
		return time.Now().UTC().Add(time.Duration(expire) * time.Second)
	case parserModel.PX:
		return time.Now().UTC().Add(time.Duration(expire) * time.Millisecond)
	case parserModel.EXAT:
		return time.Unix(int64(expire), 0).UTC()
	case parserModel.PXAT:
		return time.Unix(0, int64(expire)*int64(time.Millisecond)).UTC()
	default:
		return time.Time{}
	}
}

func formatCommandOutput(resp string, cmdName string) parserModel.CommandOutput {
	return parserModel.CommandOutput{
		ComamndName: cmdName,
		Response:    resp,
	}
}

func splitByMultiple(str string, delimiters string) []string {

	// Remove empty strings
	var newParts []string

	// Find all matches in the response string
	commands := parserModel.RegexFullResyncPattern.FindAllStringSubmatch(str, -1)

	if len(commands) == 1 {
		// Extract the matched groups
		for i := 1; i < len(commands[0]); i++ {
			newParts = append(newParts, commands[0][i])
		}
	}

	// Split by delimiters
	split := func(r rune) bool {
		return strings.ContainsRune(delimiters, r)
	}

	parts := strings.FieldsFunc(str, split)

	for _, part := range parts {
		if part != "" {
			newParts = append(newParts, part)
		}
	}

	// Assume that the delimiters are single characters
	if len(delimiters) == 1 {
		for i := 0; i < len(newParts); i++ {
			newParts[i] = delimiters + parts[i]
		}
	}

	return newParts
}
