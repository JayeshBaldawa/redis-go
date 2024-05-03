package models

const (
	SIMPLE  = "+"
	ERROR   = "-ERR "
	BULK    = "$"
	ARRAYS  = "*"
	INTEGER = ":"
)

const (
	STR_WRAPPER    = "\r\n"
	FIRST_BYTE     = "$"
	REPLICATION    = "# Replication\nrole:%s\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\n"
	FULLRESYNC     = "FULLRESYNC"
	EMPTY_RDB_FILE = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

const (
	ECHO_COMMAND         = "echo"
	PING_COMMAND         = "ping"
	SET_COMMAND          = "set"
	GET_COMMAND          = "get"
	INFO_COMMAND         = "info"
	INFO_REPLICATION     = "replication"
	REPLCONF             = "replconf"
	REPLCONF_LISTEN_PORT = "listening-port"
	REPLCONF_CAPA        = "capa"
	REPLCONF_PYSYNC2     = "psync2"
	PYSNC                = "psync"
	GETACK               = "getack"
	WAIT                 = "wait"
	ACK_RESP             = "ACK"
	TYPE_COMMAND         = "type"
	XADD_COMMAND         = "xadd"
	XRANGE_COMMAND       = "xrange"
)

const (
	EX   = "ex"   // Seconds
	PX   = "px"   // Milliseconds
	EXAT = "exat" // Unix timestamp in seconds
	PXAT = "pxat" // Unix timestamp in milliseconds
)

type CommandOutput struct {
	CommandName  string
	Response     string
	NextCommands []string
	Parameters   map[string]string
}

const (
	WAIT_TIMEOUT        = "timeout"
	WAIT_REPLICAS_COUNT = "replicas"
)
