package logger

import (
	"os"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	config "github.com/codecrafters-io/redis-starter-go/app/utility"
)

var sugar *zap.SugaredLogger

// Create Console Logger & File Logger
func init() {
	// Console Encoder Configuration
	consoleCfg := zapcore.EncoderConfig{
		LevelKey:    "level",
		TimeKey:     "time",
		MessageKey:  "msg",
		EncodeLevel: zapcore.CapitalColorLevelEncoder,
		EncodeTime:  zapcore.ISO8601TimeEncoder,
	}

	// File Encoder Configuration
	fileCfg := zapcore.EncoderConfig{
		LevelKey:    "level",
		TimeKey:     "time",
		MessageKey:  "msg",
		EncodeLevel: zapcore.CapitalLevelEncoder,
		EncodeTime:  zapcore.ISO8601TimeEncoder,
	}

	// Create console encoder
	consoleEncoder := zapcore.NewConsoleEncoder(consoleCfg)

	// Create file encoder
	fileEncoder := zapcore.NewJSONEncoder(fileCfg)

	// Create console writer

	consoleWriter := zapcore.Lock(os.Stdout)

	timeStr := time.Now().Format("2006-01-02")

	// Create Folder if not exists
	_ = os.Mkdir("logs", 0755)
	_ = os.Mkdir("logs/"+timeStr, 0755)

	// Create file name
	fileName := "logs/" + timeStr + "/" + uuid.New().String() + ".log"

	// Create file writer
	file, _ := os.Create(fileName)
	fileWriter := zapcore.Lock(file)

	// Combine console and file encoder, use multiwriter to write logs to both console and file
	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, consoleWriter, zap.DebugLevel),
		zapcore.NewCore(fileEncoder, fileWriter, zap.DebugLevel),
	)

	// Logger with Stacktrace
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	// Create a SugaredLogger, which makes it easy to log messages
	sugar = logger.Sugar()
}

func LogInfo(msg string) {
	redisConfig := config.GetRedisServerConfig()
	sugar.Infof("%s:%d: %q", redisConfig.GetServerType(), redisConfig.GetPort(), msg)
}

func LogError(err error) {
	redisConfig := config.GetRedisServerConfig()
	sugar.Errorf("%s:%d: %q", redisConfig.GetServerType(), redisConfig.GetPort(), err.Error())
}

func createLogFileIfnotExist(serverType, port string) error {
	strTime := time.Now().Format("2006-01-02")
	_, err := os.Stat("logs" + "/" + strTime + "/" + serverType + "_" + port + ".log")

	if os.IsNotExist(err) {
		_, err = os.Create("logs" + "/" + strTime + "/" + serverType + "_" + port + ".log")
		if err != nil {
			return err
		}
	}
	return nil
}
