package events

import (
	"context"
	"encoding/json"
	"log"

	"roundtrip/services/trip-service/internal/domain"
	"roundtrip/shared/contracts"
	"roundtrip/shared/messaging"

	"github.com/segmentio/kafka-go"
)

type paymentConsumer struct {
	kafka   *messaging.Kafka
	service domain.TripService
}

func NewPaymentConsumer(kafka *messaging.Kafka, service domain.TripService) *paymentConsumer {
	return &paymentConsumer{
		kafka:   kafka,
		service: service,
	}
}

func (c *paymentConsumer) Listen() error {
	return c.kafka.ConsumeMessages(messaging.NotifyPaymentSuccessGroup, func(ctx context.Context, msg kafka.Message) error {
		var message contracts.KafkaMessage
		if err := json.Unmarshal(msg.Value, &message); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return err
		}
		var payload messaging.PaymentStatusUpdateData
		if err := json.Unmarshal(message.Data, &payload); err != nil {
			log.Printf("Failed to unmarshal payload: %v", err)
			return err
		}

		log.Printf("Trip has been completed and payed.")

		return c.service.UpdateTrip(
			ctx,
			payload.TripID,
			"payed",
			nil,
		)
	})
}
