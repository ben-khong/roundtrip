package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"roundtrip/services/trip-service/internal/infrastructure/events"
	"roundtrip/services/trip-service/internal/infrastructure/grpc"
	"roundtrip/services/trip-service/internal/infrastructure/repository"
	"roundtrip/services/trip-service/internal/service"
	"roundtrip/shared/db"
	"roundtrip/shared/env"
	"roundtrip/shared/messaging"
	"roundtrip/shared/tracing"
	"strings"
	"syscall"

	grpcserver "google.golang.org/grpc"
)

var GrpcAddr = ":9093"

func main() {
	// Initialize Tracing
	tracerCfg := tracing.Config{
		ServiceName:    "trip-service",
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

	// Initialize MongoDB
	mongoClient, err := db.NewMongoClient(ctx, db.NewMongoDefaultConfig())
	if err != nil {
		log.Fatalf("Failed to initialize MongoDB, err: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	mongoDb := db.GetDatabase(mongoClient, db.NewMongoDefaultConfig())

	log.Printf(mongoDb.Name())

	kafkaBrokers := strings.Split(env.GetString("KAFKA_BROKERS", "kafka:9092"), ",")

	mongoDBRepo := repository.NewMongoRepository(mongoDb)
	svc := service.NewService(mongoDBRepo)

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		cancel()
	}()

	lis, err := net.Listen("tcp", GrpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Kafka connection
	kafka, err := messaging.NewKafka(kafkaBrokers)
	if err != nil {
		log.Fatal(err)
	}
	defer kafka.Close()

	log.Println("Starting Kafka connection")

	publisher := events.NewTripEventPublisher(kafka)

	// Start driver consumer
	driverConsumer := events.NewDriverConsumer(kafka, svc)
	go driverConsumer.Listen()

	// Start payment consumer
	paymentConsumer := events.NewPaymentConsumer(kafka, svc)
	go paymentConsumer.Listen()

	// Starting the gRPC server
	grpcServer := grpcserver.NewServer(tracing.WithTracingInterceptors()...)
	grpc.NewGRPCHandler(grpcServer, svc, publisher)

	log.Printf("Starting gRPC server Trip service on port %s", lis.Addr().String())

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("failed to serve: %v", err)
			cancel()
		}
	}()

	// wait for the shutdown signal
	<-ctx.Done()
	log.Println("Shutting down the server...")
	grpcServer.GracefulStop()
}
