package shiva

import (
	"fmt"
	"log"
	"log/slog"
)

// todo: still trying to determine the most flexible and useful API for this

type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
}

// SlogAdapter is a structured logging adapter wrapping a slog.Logger. It provides
// methods for logging at different levels with key-value pairs.
type SlogAdapter struct {
	logger *slog.Logger
}

// NewSlogAdapter initializes a new instance of SlogAdapter wrapping the provided
// slog.Logger.
//
// Panics if logger is nil
func NewSlogAdapter(logger *slog.Logger) *SlogAdapter {
	if logger == nil {
		panic("cannot wrap nil slog.Logger")
	}
	return &SlogAdapter{logger: logger}
}

func (s SlogAdapter) Debug(msg string, args ...interface{}) {
	s.logger.Debug(fmt.Sprintf(msg, args...))
}

func (s SlogAdapter) Info(msg string, args ...interface{}) {
	s.logger.Info(fmt.Sprintf(msg, args...))
}

func (s SlogAdapter) Warn(msg string, args ...interface{}) {
	s.logger.Warn(fmt.Sprintf(msg, args...))
}

func (s SlogAdapter) Error(msg string, args ...interface{}) {
	s.logger.Error(fmt.Sprintf(msg, args...))
}

type StdLogAdapter struct {
	logger *log.Logger
}

func NewStdLogAdapter(logger *log.Logger) *StdLogAdapter {
	if logger == nil {
		panic("cannot wrap nil log.Logger")
	}
	return &StdLogAdapter{logger: logger}
}

func (s StdLogAdapter) Debug(format string, args ...interface{}) {
	s.logger.Printf("[DEBUG] "+format, args...)
}

func (s StdLogAdapter) Info(format string, args ...interface{}) {
	s.logger.Printf("[INFO] "+format, args...)
}

func (s StdLogAdapter) Warn(format string, args ...interface{}) {
	s.logger.Printf("[WARN] "+format, args...)
}

func (s StdLogAdapter) Error(format string, args ...interface{}) {
	s.logger.Printf("[ERROR] "+format, args...)
}

// NopLogger is a no-operation logger that implements logging methods without
// performing any actual logging.
type NopLogger struct{}

func NewNopLogger() *NopLogger {
	return &NopLogger{}
}

func (n NopLogger) Debug(_ string, _ ...interface{}) {}

func (n NopLogger) Info(_ string, _ ...interface{}) {}

func (n NopLogger) Warn(_ string, _ ...interface{}) {}

func (n NopLogger) Error(_ string, _ ...interface{}) {}

// var (
// 	logger *slog.Logger
// 	level  *slog.LevelVar
// )
//
// func init() {
// 	level = new(slog.LevelVar)
// 	level.Set(parseLogLevel(os.Getenv("SHIVA_LOG_LEVEL")))
// 	handler := slogHandler(os.Getenv("SHIVA_LOG_FORMAT"))
// 	logger = slog.New(handler)
// }
//
// func parseLogLevel(level string) slog.Level {
// 	switch strings.ToUpper(level) {
// 	case "DEBUG":
// 		return slog.LevelDebug
// 	case "INFO":
// 		return slog.LevelInfo
// 	case "WARN":
// 		return slog.LevelWarn
// 	case "ERROR":
// 		return slog.LevelError
// 	default:
// 		// If the level provided is empty or invalid the default behavior is to log
// 		// errors only
// 		return slog.LevelError
// 	}
// }
//
// func slogHandler(format string) slog.Handler {
// 	switch strings.ToUpper(format) {
// 	case "JSON":
// 		return slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
// 			AddSource: false,
// 			Level:     level,
// 		})
// 	case "TEXT":
// 		return slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
// 			AddSource: false,
// 			Level:     level,
// 		})
// 	default:
// 		// Default to JSON logging
// 		return slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
// 			AddSource: false,
// 			Level:     level,
// 		})
// 	}
// }
//
// // SetLogger replaces the default logger for shiva.
// func SetLogger(l *slog.Logger) {
// 	logger = l
// }
