package shivaotel

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	Scope   = "github.com/jkratz55/shiva"
	Version = "0.1.0"

	labelHandler   = "handler"
	labelTopic     = "topic"
	labelPartition = "partition"
	labelGroup     = "groupId"
	labelCode      = "code"
)

type SpanWrapper struct {
	trace.Span
}

func (sp *SpanWrapper) SetAttributes(attrs ...any) {
	sp.Span.SetAttributes(attribute.String("", ""))
}

func (sp *SpanWrapper) RecordError(err error) {
	sp.Span.SetStatus(codes.Error, err.Error())
	sp.Span.RecordError(err)
}

func (sp *SpanWrapper) EndSpan() {
	sp.End()
}
