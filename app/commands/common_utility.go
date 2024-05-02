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

func encodeNoneTypeString() string {
	return encodeSimpleString("none")
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

func encodeErrorString(err error) string {
	return parserModel.ERROR + " " + err.Error() + parserModel.STR_WRAPPER
}

func encodeIntegerString(resp int) string {
	return fmt.Sprintf("%s%d%s", parserModel.INTEGER, resp, parserModel.STR_WRAPPER)
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

func formatCommandOutput(resp string, cmdName string, parameters map[string]string) parserModel.CommandOutput {
	return parserModel.CommandOutput{
		CommandName: cmdName,
		Response:    resp,
		Parameters:  parameters,
	}
}
