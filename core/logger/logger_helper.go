package logger

import (
	"os"
	"strings"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	loggerInstance *zap.Logger
	loggerMu       sync.RWMutex
)

// getLogger creates or returns a logger instance configured by environment
// variables: LOGGER_LEVEL, LOGGER_FORMAT and LOGGER_PATH.
func getLogger() *zap.Logger {
	loggerMu.RLock()
	if loggerInstance != nil {
		loggerMu.RUnlock()
		return loggerInstance
	}
	loggerMu.RUnlock()

	loggerMu.Lock()
	defer loggerMu.Unlock()

	// Double check
	if loggerInstance != nil {
		return loggerInstance
	}

	logLevel := strings.TrimSpace(os.Getenv("LOGGER_LEVEL"))
	if logLevel == "" {
		logLevel = "info"
	}
	timeFormat := strings.TrimSpace(os.Getenv("LOGGER_FORMAT"))
	if timeFormat == "" {
		timeFormat = "2006-01-02 15:04:05"
	}
	logPath := normalizeZapOutputPath(strings.TrimSpace(os.Getenv("LOGGER_PATH")))

	level := zap.NewAtomicLevel()
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	}

	outputPaths := dedupePaths([]string{"stdout", logPath})
	errorPaths := dedupePaths(outputPaths)

	zapLogger, err := zap.Config{
		Level:             level,
		Development:       false,
		Encoding:          "json",
		DisableStacktrace: false,
		DisableCaller:     false,
		OutputPaths:       outputPaths,
		ErrorOutputPaths:  errorPaths,
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			EncodeTime:     zapcore.TimeEncoderOfLayout(timeFormat),
			EncodeDuration: zapcore.StringDurationEncoder,
			LevelKey:       "Level",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			NameKey:        "key",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "Message",
			LineEnding:     zapcore.DefaultLineEnding,
		},
	}.Build()

	if err != nil {
		zapLogger, _ = zap.Config{
			Level:       zap.NewAtomicLevelAt(zap.ErrorLevel),
			Development: false,
			Encoding:    "json",
			OutputPaths: []string{"stdout"},
			EncoderConfig: zapcore.EncoderConfig{
				TimeKey:     "ts",
				EncodeTime:  zapcore.ISO8601TimeEncoder,
				LevelKey:    "level",
				EncodeLevel: zapcore.CapitalLevelEncoder,
				MessageKey:  "msg",
				LineEnding:  zapcore.DefaultLineEnding,
			},
		}.Build()
	}

	loggerInstance = zapLogger
	return loggerInstance
}

// GetLogger exposes the shared package logger for sibling moved-layout packages.
func GetLogger() *zap.Logger {
	return getLogger()
}

func normalizeZapOutputPath(p string) string {
	p = strings.TrimSpace(p)
	switch p {
	case "", "stdout", "/dev/stdout", "/dev/fd/1", "fd://1", "pipe:1":
		return "stdout"
	case "stderr", "/dev/stderr", "/dev/fd/2", "fd://2", "pipe:2":
		return "stderr"
	default:
		return p
	}
}

func dedupePaths(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	var out []string
	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}
