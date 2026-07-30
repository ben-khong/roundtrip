package main

import (
	"context"
	"encoding/json"
	"log"
	"roundtrip/shared/contracts"
	"roundtrip/shared/messaging"

	"github.com/segmentio/kafka-go"
)

type tripConsumer struct {
	kafka   *messaging.Kafka
	service *Service
}

func NewTripConsumer(kafka *messaging.Kafka, service *Service) *tripConsumer {
	return &tripConsumer{
		kafka:   kafka,
		service: service,
	}
}

func (c *tripConsumer) Listen() error {
	return c.kafka.ConsumeMessages(messaging.FindAvailableDriversGroup, func(ctx context.Context, msg kafka.Message) error {
		var tripEvent contracts.KafkaMessage
		if err := json.Unmarshal(msg.Value, &tripEvent); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return err
		}

		var payload messaging.TripEventData
		if err := json.Unmarshal(tripEvent.Data, &payload); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return err
		}

		log.Printf("driver received message: %+v", payload)

		switch msg.Topic {
		case contracts.TripEventCreated, contracts.TripEventDriverNotInterested:
			return c.handleFindAndNotifyDrivers(ctx, payload)
		}

		log.Printf("unknown trip event: %+v", payload)

		return nil
	})
}

func (c *tripConsumer) handleFindAndNotifyDrivers(ctx context.Context, payload messaging.TripEventData) error {
	suitableIDs := c.service.FindAvailableDrivers(payload.Trip.SelectedFare.PackageSlug)

	log.Printf("Found suitable drivers %v", len(suitableIDs))

	if len(suitableIDs) == 0 {
		// Notify the driver that no drivers are available
		if err := c.kafka.PublishMessage(ctx, contracts.TripEventNoDriversFound, contracts.KafkaMessage{
			OwnerID: payload.Trip.UserID,
		}); err != nil {
			log.Printf("Failed to publish message to Kafka: %v", err)
			return err
		}

		return nil
	}

	suitableDriverID := suitableIDs[0]

	marshalledEvent, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// Notify the driver about a potential trip
	if err := c.kafka.PublishMessage(ctx, contracts.DriverCmdTripRequest, contracts.KafkaMessage{
		OwnerID: suitableDriverID,
		Data:    marshalledEvent,
	}); err != nil {
		log.Printf("Failed to publish message to Kafka: %v", err)
		return err
	}

	return nil
}
