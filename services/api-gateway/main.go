package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"roundtrip/shared/env"
	"roundtrip/shared/messaging"
	"roundtrip/shared/tracing"
)

var (
	httpAddr     = env.GetString("HTTP_ADDR", ":8081")
	kafkaBrokers = strings.Split(env.GetString("KAFKA_BROKERS", "kafka:9092"), ",")
)

func main() {
	log.Println("Starting API Gateway")

	// Initialize Tracing
	tracerCfg := tracing.Config{
		ServiceName:    "api-gateway",
		Environment:    env.GetString("ENVIRONMENT", "development"),
		JaegerEndpoint: env.GetString("JAEGER_ENDPOINT", "http://jaeger:14268/api/traces"),
	}

	sh, err := tracing.InitTracer(tracerCfg)
	if err != nil {
		log.Fatalf("Failed to initialize the tracer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer sh(ctx)

	mux := http.NewServeMux()

	// Kafka connection
	kafka, err := messaging.NewKafka(kafkaBrokers)
	if err != nil {
		log.Fatal(err)
	}
	defer kafka.Close()

	log.Println("Starting Kafka connection")

	// Forward the rider and driver notifications to their websocket connections
	wsGroups := []string{
		messaging.NotifyDriverNoDriversFoundGroup,
		messaging.NotifyDriverAssignGroup,
		messaging.NotifyPaymentSessionCreatedGroup,
		messaging.DriverCmdTripRequestGroup,
	}

	for _, g := range wsGroups {
		consumer := messaging.NewGroupConsumer(kafka, connManager, g)

		if err := consumer.Start(); err != nil {
			log.Fatalf("Failed to start consumer for group: %s: err: %v", g, err)
		}
	}

	mux.Handle("POST /trip/preview", tracing.WrapHandlerFunc(enableCORS(handleTripPreview), "/trip/preview"))
	mux.Handle("POST /trip/start", tracing.WrapHandlerFunc(enableCORS(handleTripStart), "/trip/start"))
	mux.Handle("/ws/drivers", tracing.WrapHandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleDriversWebSocket(w, r, kafka)
	}, "/ws/drivers"))
	mux.Handle("/ws/riders", tracing.WrapHandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleRidersWebSocket(w, r)
	}, "/ws/riders"))
	mux.Handle("/webhook/stripe", tracing.WrapHandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleStripeWebhook(w, r, kafka)
	}, "/webhook/stripe"))

	server := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	serverErrors := make(chan error, 1)

	go func() {
		log.Printf("Server listening on %s", httpAddr)
		serverErrors <- server.ListenAndServe()
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		log.Printf("Error starting the server: %v", err)

	case sig := <-shutdown:
		log.Printf("Server is shutting down due to %v signal", sig)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Could not stop the server gracefully: %v", err)
			server.Close()
		}
	}
}
