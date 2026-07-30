package tracing

import (
	"context"
	"encoding/json"

	"roundtrip/shared/contracts"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// kafkaHeadersCarrier implements the TextMapCarrier interface for Kafka headers
type kafkaHeadersCarrier struct {
	headers *[]kafka.Header
}

func (c kafkaHeadersCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeadersCarrier) Set(key string, value string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c kafkaHeadersCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// TracedPublisher wraps the Kafka publish function with tracing
func TracedPublisher(ctx context.Context, msg kafka.Message, publish func(context.Context, kafka.Message) error) error {
	tracer := otel.GetTracerProvider().Tracer("kafka")

	ctx, span := tracer.Start(ctx, "kafka.publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", msg.Topic),
		),
	)
	defer span.End()

	// Try to extract and add message details to span (map[string]any if you don't know the type)
	var msgBody contracts.KafkaMessage
	if err := json.Unmarshal(msg.Value, &msgBody); err == nil {
		if msgBody.OwnerID != "" {
			span.SetAttributes(attribute.String("messaging.owner_id", msgBody.OwnerID))
		}
	}

	// Inject trace context into message headers
	otel.GetTextMapPropagator().Inject(ctx, kafkaHeadersCarrier{headers: &msg.Headers})

	if err := publish(ctx, msg); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

// TracedConsumer wraps the Kafka message handler with tracing
func TracedConsumer(groupID string, msg kafka.Message, handler func(context.Context, kafka.Message) error) error {
	// Extract trace context from message headers
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), kafkaHeadersCarrier{headers: &msg.Headers})

	tracer := otel.GetTracerProvider().Tracer("kafka")

	ctx, span := tracer.Start(ctx, "kafka.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", msg.Topic),
			attribute.String("messaging.kafka.consumer_group", groupID),
			attribute.Int("messaging.kafka.partition", msg.Partition),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		),
	)
	defer span.End()

	// Try to extract and add message details to span (map[string]any if you don't know the type)
	var msgBody contracts.KafkaMessage
	if err := json.Unmarshal(msg.Value, &msgBody); err == nil {
		if msgBody.OwnerID != "" {
			span.SetAttributes(attribute.String("messaging.owner_id", msgBody.OwnerID))
		}
	}

	if err := handler(ctx, msg); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}
