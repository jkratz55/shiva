package shiva

import (
	"log"
	"log/slog"
	"os"
	"testing"
)

func TestSlogAdapter_Debug(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource:   true,
		Level:       slog.LevelDebug,
		ReplaceAttr: nil,
	}))

	slogAdapter := NewSlogAdapter(logger)
	slogAdapter.Debug("test message", "topic", "CmsDocumentsv2", "partition", 1, "offset", 12343242)
}

func TestStdLogAdapter_Debug(t *testing.T) {
	logger := log.New(os.Stderr, log.Prefix(), log.LstdFlags)
	stdLogAdapter := NewStdLogAdapter(logger)
	stdLogAdapter.Debug("test message", "topic", "CmsDocumentsv2", "partition", 1, "offset", 12343242)
}
