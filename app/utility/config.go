package utility

type RedisServer struct {
	port        int
	replicaHost string
	replicaPort int
	serverType  string
	RDBFileDir  string
	RDBFileName string
}

const (
	MASTER_SERVER = "master"
	SLAVE_SERVER  = "slave"
	DEFAULT_PORT  = 6379
)

const (
	READ_TIMEOUT = 60 // seconds
)

var redisServerConfig *RedisServer

func init() {
	redisServerConfig = &RedisServer{
		port:        DEFAULT_PORT,
		replicaHost: "",
		replicaPort: 0,
		serverType:  MASTER_SERVER,
		RDBFileDir:  "/tmp/",
		RDBFileName: "dump.rdb",
	}
}

func GetRedisServerConfig() *RedisServer {
	return redisServerConfig
}

func (r *RedisServer) GetPort() int {
	return r.port
}

func (r *RedisServer) GetReplicaHost() string {
	return r.replicaHost
}

func (r *RedisServer) GetReplicaPort() int {
	return r.replicaPort
}

func (r *RedisServer) GetServerType() string {
	return r.serverType
}

func (r *RedisServer) SetPort(port int) {
	r.port = port
}

func (r *RedisServer) SetReplicaHost(replicaHost string) {
	r.replicaHost = replicaHost
}

func (r *RedisServer) SetReplicaPort(replicaPort int) {
	r.replicaPort = replicaPort
}

func (r *RedisServer) SetServerType(serverType string) {
	r.serverType = serverType
}

func (r *RedisServer) IsMaster() bool {
	return r.serverType == MASTER_SERVER
}

func (r *RedisServer) IsSlave() bool {
	return r.serverType == SLAVE_SERVER
}

func GetReadTimeout() int {
	return READ_TIMEOUT
}

func (r *RedisServer) GetRDBFileDir() string {
	return r.RDBFileDir
}

func (r *RedisServer) GetRDBFileName() string {
	return r.RDBFileName
}

func (r *RedisServer) SetRDBFileDir(dir string) {
	r.RDBFileDir = dir
}

func (r *RedisServer) SetRDBFileName(name string) {
	r.RDBFileName = name
}
