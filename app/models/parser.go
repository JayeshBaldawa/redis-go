package models

const (
	SIMPLE = "+"
	ERROR  = "-"
	BULK   = "$"
	ARRAYS = "*"
)

const (
	STR_WRAPPER = "\r\n"
	FIRST_BYTE  = "$"
	REPLICATION = "# Replication\nrole:%s\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\n"
)

const (
	ECHO_COMMAND     = "echo"
	PING_COMMAND     = "ping"
	SET_COMMAND      = "set"
	GET_COMMAND      = "get"
	INFO_COMMAND     = "info"
	INFO_REPLICATION = "replication"
)

const (
	EX   = "ex"   // Seconds
	PX   = "px"   // Milliseconds
	EXAT = "exat" // Unix timestamp in seconds
	PXAT = "pxat" // Unix timestamp in milliseconds
)
