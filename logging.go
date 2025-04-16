package shiva

import (
	"fmt"
	"log"
	"log/slog"
)

type Logger interface {
	Debug(msg string, kvs ...interface{})
	Info(msg string, kvs ...interface{})
	Warn(msg string, kvs ...interface{})
	Error(msg string, kvs ...interface{})
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

func (s SlogAdapter) Debug(msg string, kvs ...interface{}) {
	s.logger.Debug(msg, kvs...)
}

func (s SlogAdapter) Info(msg string, kvs ...interface{}) {
	s.logger.Info(msg, kvs...)
}

func (s SlogAdapter) Warn(msg string, kvs ...interface{}) {
	s.logger.Warn(msg, kvs...)
}

func (s SlogAdapter) Error(msg string, kvs ...interface{}) {
	s.logger.Error(msg, kvs...)
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

func (s StdLogAdapter) Debug(msg string, kvs ...interface{}) {
	s.logger.Printf("[DEBUG] %s %s", msg, formatKVs(kvs...))
}

func (s StdLogAdapter) Info(msg string, kvs ...interface{}) {
	s.logger.Printf("[INFO] %s %s", msg, formatKVs(kvs...))
}

func (s StdLogAdapter) Warn(msg string, kvs ...interface{}) {
	s.logger.Printf("[WARN] %s %s", msg, formatKVs(kvs...))
}

func (s StdLogAdapter) Error(msg string, kvs ...interface{}) {
	s.logger.Printf("[ERROR] %s %s", msg, formatKVs(kvs...))
}

func formatKVs(kvs ...interface{}) string {
	if len(kvs) == 0 {
		return ""
	}
	formatted := ""
	for i := 0; i < len(kvs); i += 2 {
		key := kvs[i]
		var value interface{} = "(missing)"
		if i+1 < len(kvs) {
			value = kvs[i+1]
		}
		formatted += fmt.Sprintf("%v=%v ", key, value)
	}
	return formatted
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
