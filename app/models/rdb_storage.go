package models

const (
	RDB_MAGIC_NUMBER           = "REDIS"
	RDB_DATABASE_SELECT_OPCODE = 0xFE
	RDB_END_OPCODE             = 0xFF
	RDB_OPCODE_EXPIRETIME_MS   = 0xFC
	RDB_OPCODE_EXPIRETIME      = 0xFD
	RDB_OPCODE_SELECTDB        = 0xFE
	RDB_OPCODE_RESIZEDB        = 0xFB
)

// Length Encoding Constants
const (
	// 00
	RDB_ENC_INT8 = 0b00
	// 01
	RDB_ENC_INT16 = 0b01
	// 10
	RDB_ENC_INT32 = 0b10
	// 11
	RDB_ENC_LZF = 0b11
)
