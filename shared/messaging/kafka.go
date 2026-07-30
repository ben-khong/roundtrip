package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"roundtrip/shared/contracts"
	"roundtrip/shared/retry"
	"roundtrip/shared/tracing"

	"github.com/segmentio/kafka-go"
)

const (
	// Number of partitions per topic. Messages are keyed by owner ID, so all
	// messages for the same user land on the same partition and stay ordered.
	topicPartitions        = 3
	topicReplicationFactor = 1
)

// consumerGroupTopics maps each consumer group to the topics it subscribes to.
// Every consumer group receives its own copy of each message on its topics,
// and the members of one group share the work between them.
var consumerGroupTopics = map[string][]string{
	FindAvailableDriversGroup:        {contracts.TripEventCreated, contracts.TripEventDriverNotInterested},
	DriverCmdTripRequestGroup:        {contracts.DriverCmdTripRequest},
	DriverTripResponseGroup:          {contracts.DriverCmdTripAccept, contracts.DriverCmdTripDecline},
	NotifyDriverNoDriversFoundGroup:  {contracts.TripEventNoDriversFound},
	NotifyDriverAssignGroup:          {contracts.TripEventDriverAssigned},
	PaymentTripResponseGroup:         {contracts.PaymentCmdCreateSession},
	NotifyPaymentSessionCreatedGroup: {contracts.PaymentEventSessionCreated},
	NotifyPaymentSuccessGroup:        {contracts.PaymentEventSuccess},
}

type Kafka struct {
	brokers []string
	writer  *kafka.Writer

	mu      sync.Mutex
	readers []*kafka.Reader
}

func NewKafka(brokers []string) (*Kafka, error) {
	if len(brokers) == 0 {
		return nil, errors.New("no Kafka brokers configured")
	}

	k := &Kafka{
		brokers: brokers,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			// Publish right away instead of waiting to fill a batch
			BatchTimeout: 10 * time.Millisecond,
		},
	}

	if err := k.setupTopics(); err != nil {
		// Clean up if setup fails
		k.Close()
		return nil, fmt.Errorf("failed to setup topics: %v", err)
	}

	return k, nil
}

type MessageHandler func(context.Context, kafka.Message) error

func (k *Kafka) ConsumeMessages(groupID string, handler MessageHandler) error {
	reader, err := k.newReader(groupID)
	if err != nil {
		return err
	}

	go func() {
		for {
			// Fetch without committing, the offset is only committed once the message is handled.
			// Messages are processed one at a time per consumer.
			msg, err := reader.FetchMessage(context.Background())
			if err != nil {
				if errors.Is(err, io.EOF) {
					return // Reader was closed
				}
				log.Printf("Failed to fetch message for group %s: %v", groupID, err)
				continue
			}

			if err := tracing.TracedConsumer(groupID, msg, func(ctx context.Context, m kafka.Message) error {
				log.Printf("Received a message: %s", m.Value)

				cfg := retry.DefaultConfig()
				err := retry.WithBackoff(ctx, cfg, func() error {
					return handler(ctx, m)
				})
				if err != nil {
					log.Printf("Message processing failed after %d retries for message %s/%d/%d, err: %v", cfg.MaxRetries, m.Topic, m.Partition, m.Offset, err)

					if dlqErr := k.publishToDeadLetter(ctx, groupID, m, err, cfg.MaxRetries); dlqErr != nil {
						log.Printf("ERROR: Failed to publish message to the DLQ: %v. Message body: %s", dlqErr, m.Value)
					}
				}

				// Commit even on failure so a poisoned message doesn't block the partition,
				// failed messages are kept in the DLQ instead.
				if commitErr := reader.CommitMessages(ctx, m); commitErr != nil {
					log.Printf("ERROR: Failed to commit message: %v. Message body: %s", commitErr, m.Value)
				}
				return err
			}); err != nil {
				log.Printf("Error processing message: %v", err)
			}
		}
	}()

	return nil
}

func (k *Kafka) PublishMessage(ctx context.Context, topic string, message contracts.KafkaMessage) error {
	log.Printf("Publishing message to topic: %s", topic)

	jsonMsg, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	msg := kafka.Message{
		Topic: topic,
		Value: jsonMsg,
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
		},
	}
	if message.OwnerID != "" {
		msg.Key = []byte(message.OwnerID)
	}

	return tracing.TracedPublisher(ctx, msg, k.publish)
}

func (k *Kafka) publish(ctx context.Context, msg kafka.Message) error {
	return k.writer.WriteMessages(ctx, msg)
}

// publishToDeadLetter sends a message that failed processing to the DLQ, with the failure context in its headers
func (k *Kafka) publishToDeadLetter(ctx context.Context, groupID string, msg kafka.Message, cause error, retries int) error {
	headers := make([]kafka.Header, 0, len(msg.Headers)+6)
	headers = append(headers, msg.Headers...)
	headers = append(headers,
		kafka.Header{Key: "x-death-reason", Value: []byte(cause.Error())},
		kafka.Header{Key: "x-original-topic", Value: []byte(msg.Topic)},
		kafka.Header{Key: "x-original-partition", Value: []byte(strconv.Itoa(msg.Partition))},
		kafka.Header{Key: "x-original-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		kafka.Header{Key: "x-consumer-group", Value: []byte(groupID)},
		kafka.Header{Key: "x-retry-count", Value: []byte(strconv.Itoa(retries))},
	)

	return k.writer.WriteMessages(ctx, kafka.Message{
		Topic:   DeadLetterTopic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

func (k *Kafka) newReader(groupID string) (*kafka.Reader, error) {
	topics, ok := consumerGroupTopics[groupID]
	if !ok {
		return nil, fmt.Errorf("unknown consumer group: %s", groupID)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     k.brokers,
		GroupID:     groupID,
		GroupTopics: topics,
		StartOffset: kafka.FirstOffset,
	})

	k.mu.Lock()
	k.readers = append(k.readers, reader)
	k.mu.Unlock()

	return reader, nil
}

func (k *Kafka) setupTopics() error {
	conn, err := kafka.Dial("tcp", k.brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka: %v", err)
	}
	defer conn.Close()

	// Topics have to be created on the controller broker
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %v", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer controllerConn.Close()

	seen := map[string]bool{DeadLetterTopic: true}
	configs := []kafka.TopicConfig{newTopicConfig(DeadLetterTopic)}
	for _, topics := range consumerGroupTopics {
		for _, topic := range topics {
			if !seen[topic] {
				seen[topic] = true
				configs = append(configs, newTopicConfig(topic))
			}
		}
	}

	// Topics that already exist are skipped
	if err := controllerConn.CreateTopics(configs...); err != nil {
		return fmt.Errorf("failed to create topics: %v", err)
	}

	return nil
}

func newTopicConfig(topic string) kafka.TopicConfig {
	return kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     topicPartitions,
		ReplicationFactor: topicReplicationFactor,
	}
}

func (k *Kafka) Close() {
	k.mu.Lock()
	defer k.mu.Unlock()

	for _, r := range k.readers {
		r.Close()
	}
	if k.writer != nil {
		k.writer.Close()
	}
}
