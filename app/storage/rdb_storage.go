package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"time"

	parseModel "github.com/codecrafters-io/redis-starter-go/app/models"
	config "github.com/codecrafters-io/redis-starter-go/app/utility"
)

type RDBStorage struct{}

var rdbStorage *RDBStorage

func GetRDBStorage() *RDBStorage {
	if rdbStorage == nil {
		rdbStorage = &RDBStorage{}
	}
	return rdbStorage
}

func (r *RDBStorage) LoadRDBFile() error {

	// Get the RDB file path
	rdbFilePath := config.GetRedisServerConfig().GetRDBFileDir() + "/" + config.GetRedisServerConfig().GetRDBFileName()

	file, err := os.Open(rdbFilePath)
	if err != nil {
		fmt.Println("Error opening the RDB file: ", err)
		return nil
	}

	defer file.Close()

	reader := bufio.NewReader(file)

	// Check the magic number
	err = r.checkMagicNumber(reader)
	if err != nil {
		return err
	}

	// Skip the metadata
	err = r.skipMetadata(reader)
	if err != nil {
		return err
	}

	// Check the database number
	err = r.checkDatabaseNumber(reader)
	if err != nil {
		return err
	}

	// Now, start reading the RDB file
	for {
		opcode, err := reader.ReadByte()
		if err != nil {
			return err
		}

		switch opcode {
		case parseModel.RDB_END_OPCODE: // End of the RDB file
			return nil

		case parseModel.RDB_OPCODE_SELECTDB:
			err = r.readSelectDB(reader)
			if err != nil {
				return err
			}

		case parseModel.RDB_OPCODE_RESIZEDB:
			err = r.readResizeDB(reader)
			if err != nil {
				return err
			}

		default:
			expiryTime := int64(-1)
			expiryValueType := opcode
			expiryTimeType := ""

			switch opcode {
			case parseModel.RDB_OPCODE_EXPIRETIME_MS:
				expiryTime, expiryValueType, err = r.readExpireTimeMS(reader)
				if err != nil {
					return err
				}
				expiryTimeType = parseModel.PX
			case parseModel.RDB_OPCODE_EXPIRETIME:
				expiryTime, expiryValueType, err = r.readExpireTime(reader)
				if err != nil {
					return err
				}
				expiryTimeType = parseModel.EX
			}

			if expiryValueType != 0x00 {
				continue
			}

			// Length Encoding
			keyLength, err := r.lengthEncodedInt(reader)
			if err != nil {
				return err
			}

			keyBytes := make([]byte, keyLength)
			_, err = reader.Read(keyBytes)
			if err != nil {
				return err
			}

			// Length Encoding
			valueLength, err := r.lengthEncodedInt(reader)
			if err != nil {
				return err
			}

			valueBytes := make([]byte, valueLength)
			_, err = reader.Read(valueBytes)
			if err != nil {
				return err
			}

			fmt.Printf("Key: %s, Value: %s\n ", string(keyBytes), string(valueBytes))

			GetStorage().Set(string(keyBytes), string(valueBytes), getExpiryTimeInUTC(int(expiryTime), expiryTimeType))
		}
	}

}

/*
	The file header consists of two parts: the Magic Number and the version number
	- RDB files start with the ASCII-encoded 'REDIS' as the File Magic Number to represent their file type
	- The next 4 bytes represent the version number of the RDB file
*/

func (r *RDBStorage) checkMagicNumber(reader *bufio.Reader) error {

	// 52 45 44 49 53              # Magic String "REDIS"

	magicNumber, err := reader.Peek(5)
	if err != nil {
		return err
	}

	if string(magicNumber) != parseModel.RDB_MAGIC_NUMBER {
		return fmt.Errorf("invalid RDB file format")
	}

	// Move the reader to the next position
	_, err = reader.Discard(5)
	if err != nil {
		return err
	}

	// 30 30 30 33                 # RDB Version Number as ASCII string. "0003" = 3

	versionNumber, err := reader.Peek(4)
	if err != nil {
		return err
	}

	fmt.Println("RDB Version Number: ", string(versionNumber))

	// If everything is fine, return nil
	_, err = reader.Discard(4)
	if err != nil {
		return err
	}

	return nil
}

func (r *RDBStorage) checkDatabaseNumber(reader *bufio.Reader) error {

	// FE 00                       # Indicates database selector. db number = 00

	dbNumber, err := reader.ReadByte()
	if err != nil {
		return err
	}

	fmt.Println("Database Number: ", dbNumber)

	return nil
}

func (r *RDBStorage) skipMetadata(reader *bufio.Reader) error {

	maxBytesToRead := 1024
	currentBytesRead := 0

	// skip to 0xFE opcode
	for {
		currentBytesRead++
		opcode, err := reader.ReadByte()
		if err != nil {
			return err
		}

		if opcode == parseModel.RDB_DATABASE_SELECT_OPCODE {
			break
		}

		if currentBytesRead >= maxBytesToRead {
			return fmt.Errorf("invalid RDB file format")
		}

		if opcode == parseModel.RDB_END_OPCODE {
			return fmt.Errorf("invalid RDB file format")
		}
	}

	return nil
}

func (r *RDBStorage) readExpireTimeMS(reader *bufio.Reader) (int64, byte, error) {
	// Peek 8 bytes from the reader for the expiry time
	expiryBytes, err := reader.Peek(8)
	if err != nil {
		return 0, 0, err
	}

	// Discard the peeked bytes (8 bytes)
	if _, err := reader.Discard(8); err != nil {
		return 0, 0, err
	}

	// Convert the 8-byte expiry bytes to a uint64 (little-endian assumed)
	expirySeconds := int64(binary.LittleEndian.Uint64(expiryBytes))

	// Calculate expiry time in milliseconds since epoch
	expiryMilliseconds := expirySeconds * 1000 // Convert seconds to milliseconds

	// Read the value type byte
	valueType, err := reader.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	return expiryMilliseconds, valueType, nil
}

func (r *RDBStorage) readExpireTime(reader *bufio.Reader) (int64, byte, error) {

	expiryBytes, err := reader.ReadBytes(4)
	if err != nil {
		return 0, 0, err
	}

	expiry := int64(binary.LittleEndian.Uint32(expiryBytes))
	valueType, err := reader.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	return expiry, valueType, nil
}

func (r *RDBStorage) readSelectDB(reader *bufio.Reader) error {

	// FE <database-id>             # Select the database to associate the following keys with.

	dbNumber, err := reader.ReadByte()
	if err != nil {
		return err
	}

	fmt.Println("Database Number to Associate the Following Keys With: ", dbNumber)

	_, err = reader.ReadByte()
	if err != nil {
		return err
	}

	return nil
}

func (r *RDBStorage) readResizeDB(reader *bufio.Reader) error {

	// FB <length> <length>         # Resize database

	// Number of keys in the database (Length Encoding)
	numOfKeys, err := r.lengthEncodedInt(reader)
	if err != nil {
		return err
	}
	fmt.Println("Number of Keys in the Database: ", numOfKeys)

	// Number of keys with an expire time set
	numOfKeysWithExpireTime, err := r.lengthEncodedInt(reader)
	if err != nil {
		return err
	}
	fmt.Println("Number of Keys with an Expire Time Set: ", numOfKeysWithExpireTime)

	return nil
}

/*
	Bits	How to parse
	00	The next 6 bits represent the length
	01	Read one additional byte. The combined 14 bits represent the length
	10	Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
	11	The next object is encoded in a special format. The remaining 6 bits indicate the format. May be used to store numbers or Strings, see String Encoding
*/

func (r *RDBStorage) lengthEncodedInt(reader *bufio.Reader) (int, error) {

	opcode, err := reader.ReadByte()
	if err != nil {
		return -1, err
	}

	// Represented the opcode in little endian -- 2 most significant bits
	switch opcode >> 6 {
	case parseModel.RDB_ENC_INT8:
		// It's 00, so read the next 6 bits
		return int(binary.LittleEndian.Uint16([]byte{opcode, 00})), nil

	case parseModel.RDB_ENC_INT16:
		// It's 01, so read one additional byte
		int16Byte, err := reader.ReadByte()
		if err != nil {
			return -1, err
		}
		return int(binary.LittleEndian.Uint16([]byte{opcode & 0x3F, int16Byte})), nil

	case parseModel.RDB_ENC_INT32:
		// It's 10, so discard the remaining 6 bits
		int32Bytes, err := reader.Peek(4)
		if err != nil {
			return -1, err
		}

		_, err = reader.Discard(4)
		if err != nil {
			return -1, err
		}

		return int(binary.LittleEndian.Uint32([]byte{opcode & 0x3F, int32Bytes[0], int32Bytes[1], int32Bytes[2], int32Bytes[3]})), nil

	case parseModel.RDB_ENC_LZF:
		// It's 11, so the next object is encoded in a special format
		// The remaining 6 bits indicate the format
		switch opcode & 0x3F {
		case 0:
			buf := make([]byte, 1)
			_, err := reader.Read(buf)
			if err != nil {
				return -1, err
			}
			return int(binary.LittleEndian.Uint16([]byte{buf[0], 00})), nil

		case 1:
			buf := make([]byte, 2)
			_, err := reader.Read(buf)
			if err != nil {
				return -1, err
			}
			return int(binary.LittleEndian.Uint16(buf)), nil

		case 2:
			buf := make([]byte, 4)
			_, err := reader.Read(buf)
			if err != nil {
				return -1, err
			}
			return int(binary.LittleEndian.Uint32(buf)), nil
		}
	}

	return -1, nil
}

// Need to move this to a utility package later
func getExpiryTimeInUTC(expire int, Timetype string) time.Time {
	fmt.Println("Expire: ", expire, " Time Type: ", Timetype)
	switch strings.ToLower(Timetype) {
	case parseModel.EX:
		return time.Now().UTC().Add(time.Duration(expire) * time.Second)
	case parseModel.PX:
		fmt.Printf("Current Time: %v\n", time.Now().UTC())
		ex := time.Now().UTC().Add(time.Duration(expire) * time.Millisecond)
		fmt.Printf("Expiry Time: %v\n", ex)
		return ex
	case parseModel.EXAT:
		return time.Unix(int64(expire), 0).UTC()
	case parseModel.PXAT:
		return time.Unix(0, int64(expire)*int64(time.Millisecond)).UTC()
	default:
		return time.Time{}
	}
}
