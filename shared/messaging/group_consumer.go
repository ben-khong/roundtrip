package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"

	"roundtrip/shared/contracts"
)

// GroupConsumer forwards the messages of a consumer group to the websocket connection of their owner
type GroupConsumer struct {
	kafka   *Kafka
	connMgr *ConnectionManager
	groupID string
}

func NewGroupConsumer(kafka *Kafka, connMgr *ConnectionManager, groupID string) *GroupConsumer {
	return &GroupConsumer{
		kafka:   kafka,
		connMgr: connMgr,
		groupID: groupID,
	}
}

func (gc *GroupConsumer) Start() error {
	reader, err := gc.kafka.newReader(gc.groupID)
	if err != nil {
		return err
	}

	go func() {
		for {
			// ReadMessage commits the offset right away
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				if errors.Is(err, io.EOF) {
					return // Reader was closed
				}
				log.Printf("Failed to read message for group %s: %v", gc.groupID, err)
				continue
			}

			var msgBody contracts.KafkaMessage
			if err := json.Unmarshal(msg.Value, &msgBody); err != nil {
				log.Println("Failed to unmarshal message:", err)
				continue
			}

			userID := msgBody.OwnerID

			var payload any
			if msgBody.Data != nil {
				if err := json.Unmarshal(msgBody.Data, &payload); err != nil {
					log.Println("Failed to unmarshal payload:", err)
					continue
				}
			}

			clientMsg := contracts.WSMessage{
				Type: msg.Topic,
				Data: payload,
			}

			if err := gc.connMgr.SendMessage(userID, clientMsg); err != nil {
				log.Printf("Failed to send message to user %s: %v", userID, err)
			}
		}
	}()

	return nil
}
