package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"roundtrip/services/trip-service/internal/domain"
	"roundtrip/shared/contracts"
	"roundtrip/shared/messaging"
	pbd "roundtrip/shared/proto/driver"

	"github.com/segmentio/kafka-go"
)

type driverConsumer struct {
	kafka   *messaging.Kafka
	service domain.TripService
}

func NewDriverConsumer(kafka *messaging.Kafka, service domain.TripService) *driverConsumer {
	return &driverConsumer{
		kafka:   kafka,
		service: service,
	}
}

func (c *driverConsumer) Listen() error {
	return c.kafka.ConsumeMessages(messaging.DriverTripResponseGroup, func(ctx context.Context, msg kafka.Message) error {
		var message contracts.KafkaMessage
		if err := json.Unmarshal(msg.Value, &message); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return err
		}

		var payload messaging.DriverTripResponseData
		if err := json.Unmarshal(message.Data, &payload); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return err
		}

		log.Printf("driver response received message: %+v", payload)

		switch msg.Topic {
		case contracts.DriverCmdTripAccept:
			if err := c.handleTripAccepted(ctx, payload.TripID, payload.Driver); err != nil {
				log.Printf("Failed to handle the trip accept: %v", err)
				return err
			}
		case contracts.DriverCmdTripDecline:
			log.Println("Declined")
			return nil
		}
		log.Printf("unknown trip event: %+v", payload)

		return nil
	})
}

func (c *driverConsumer) handleTripAccepted(ctx context.Context, tripID string, driver *pbd.Driver) error {
	// 1. Fetch the first
	trip, err := c.service.GetTripByID(ctx, tripID)
	if err != nil {
		return err
	}

	if trip == nil {
		return fmt.Errorf("Trip was not found %s", tripID)
	}

	// 2. Update the trip
	if err := c.service.UpdateTrip(ctx, tripID, "accepted", driver); err != nil {
		log.Printf("Failed to update the trip: %v", err)
		return err
	}

	trip, err = c.service.GetTripByID(ctx, tripID)
	if err != nil {
		return err
	}

	// 3. Driver has been assigned -> publish this event to Kafka
	marshalledTrip, err := json.Marshal(trip)
	if err != nil {
		return err
	}

	// Notify the rider that a driver has been assigned
	if err := c.kafka.PublishMessage(ctx, contracts.TripEventDriverAssigned, contracts.KafkaMessage{
		OwnerID: trip.UserID,
		Data:    marshalledTrip,
	}); err != nil {
		return err
	}

	marshalledPayload, err := json.Marshal(messaging.PaymentTripResponseData{
		TripID:   tripID,
		UserID:   trip.UserID,
		DriverID: driver.Id,
		Amount:   trip.RideFare.TotalPriceInCents,
		Currency: "USD",
	})

	if err := c.kafka.PublishMessage(ctx, contracts.PaymentCmdCreateSession,
		contracts.KafkaMessage{
			OwnerID: trip.UserID,
			Data:    marshalledPayload,
		},
	); err != nil {
		return err
	}

	return nil
}
