package events

import (
	"context"
	"encoding/json"
	"roundtrip/services/trip-service/internal/domain"
	"roundtrip/shared/contracts"
	"roundtrip/shared/messaging"
)

type TripEventPublisher struct {
	kafka *messaging.Kafka
}

func NewTripEventPublisher(kafka *messaging.Kafka) *TripEventPublisher {
	return &TripEventPublisher{
		kafka: kafka,
	}
}

func (p *TripEventPublisher) PublishTripCreated(ctx context.Context, trip *domain.TripModel) error {
	payload := messaging.TripEventData{
		Trip: trip.ToProto(),
	}

	tripEventJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return p.kafka.PublishMessage(ctx, contracts.TripEventCreated, contracts.KafkaMessage{
		OwnerID: trip.UserID,
		Data:    tripEventJSON,
	})
}
