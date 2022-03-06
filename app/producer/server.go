package producer

import (
	"context"
	"net"
	"time"

	"github.com/segmentio/kafka-go"

	logger "github.com/webdevolegkuprianov/kafka_producer_grpc/app/producer/logger"
	"github.com/webdevolegkuprianov/kafka_producer_grpc/model"
	pb "github.com/webdevolegkuprianov/kafka_producer_grpc/proto"
	"google.golang.org/grpc"
)

type Stream struct {
	kafkaWriter1 *kafka.Writer
	config       *model.Service
	pb.StreamServer
}

func Start(config *model.Service) error {

	ctx := context.Background()

	//init kafka producer1
	kafkaWriter1 := newProducer(ctx, config)

	//init grpc server
	if err := newGrpcServer(kafkaWriter1, config); err != nil {
		logger.ErrorLogger.Println(err)
		return err
	}

	return nil
}

func newGrpcServer(kafkaWriter1 *kafka.Writer, config *model.Service) error {

	//create listener
	listener, err := net.Listen("tcp", config.Spec.Ports.GrpcServer.Addr)
	if err != nil {
		logger.ErrorLogger.Println(err)
		return err
	}

	//create grpc server
	server := grpc.NewServer()
	protoServer := Stream{
		kafkaWriter1: kafkaWriter1,
		config:       config,
	}

	pb.RegisterStreamServer(server, protoServer)

	//start server
	if err := server.Serve(listener); err != nil {
		logger.ErrorLogger.Println("failed to serve: ", err)
		return err
	}

	return nil
}

//kafka init
func newProducer(ctx context.Context, config *model.Service) *kafka.Writer {

	//dialer := &kafka.Dialer{
	//Timeout:  10 * time.Second,
	//ClientID: config.KafkaClientId,
	//}

	//intialize the writer
	writer := kafka.NewWriter(kafka.WriterConfig{

		//broker, topic, dialer assign
		Brokers: []string{config.Spec.KafkaConf.Brokers.Broker1},
		Topic:   config.Spec.KafkaConf.Topics.Topic1,
		//Dialer:  dialer,

		//writer settings
		Async:        config.Spec.ProducerConf.Async,
		Balancer:     &kafka.LeastBytes{},
		MaxAttempts:  config.Spec.ProducerConf.MaxAttemps,
		BatchSize:    config.Spec.ProducerConf.BatchSize,
		BatchBytes:   config.Spec.ProducerConf.BatchBytes,
		BatchTimeout: time.Duration(config.Spec.ProducerConf.BatchtTimeout) * time.Second,
		WriteTimeout: time.Duration(config.Spec.ProducerConf.WriteTimeout) * time.Second,
		ReadTimeout:  time.Duration(config.Spec.ProducerConf.ReadTimeout) * time.Second,
		RequiredAcks: config.Spec.ProducerConf.RequiredAcks, // can be set to -1 (all nodes), 0 (without), or 1(only lead node)
		Logger:       logger.KafkaLogger,
	})

	return writer
}
