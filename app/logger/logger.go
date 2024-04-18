package logger

import (
	config "github.com/codecrafters-io/redis-starter-go/app/utility"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var sugar *zap.SugaredLogger

func init() {
	cfg := zap.NewProductionConfig()
	// Customize the encoder to use a more human-readable format
	cfg.Encoding = "console"                                         // Use console encoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder // Encode log level with color
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder        // Use ISO8601 time format

	logger, _ := cfg.Build()

	defer logger.Sync() // flushes buffer, if any
	sugar = logger.Sugar()
}

func LogInfo(msg string) {
	redisConfig := config.GetRedisServerConfig()
	sugar.Infof("%s:%d: %q\n", redisConfig.GetServerType(), redisConfig.GetPort(), msg)
}

func LogError(err error) {
	redisConfig := config.GetRedisServerConfig()
	sugar.Errorf("%s:%d: %q\n", redisConfig.GetServerType(), redisConfig.GetPort(), err.Error())
}
