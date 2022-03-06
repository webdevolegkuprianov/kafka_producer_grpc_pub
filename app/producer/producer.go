package producer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	logger "github.com/webdevolegkuprianov/kafka_producer_grpc/app/producer/logger"
	pb "github.com/webdevolegkuprianov/kafka_producer_grpc/proto"
)

//streams
//stream1
func (s Stream) Stream1(stream pb.Stream_Stream1Server) error {

	defer s.kafkaWriter1.Close()

	for {

		body, err := stream.Recv()
		if err != nil {
			logger.ErrorLogger.Println(err)
			return err
		}

		go func() {

			mess := kafka.Message{
				Key:   []byte("stream1"),
				Value: body.Message,
				Time:  time.Now(),
			}

			if err := s.kafkaWriter1.WriteMessages(context.Background(), mess); err != nil {
				logger.ErrorLogger.Println(err)
			}
			logger.InfoLogger.Println("message sent to kafka writer")

		}()

		time.Sleep(10 * time.Millisecond)
	}
}

//stream2
func (s Stream) Stream2(stream pb.Stream_Stream2Server) error {

	defer s.kafkaWriter1.Close()

	for {

		body, err := stream.Recv()
		if err != nil {
			logger.ErrorLogger.Println(err)
			return err
		}

		go func() {

			mess := kafka.Message{
				Key:   []byte("stream1"),
				Value: body.Message,
				Time:  time.Now(),
			}

			if err := s.kafkaWriter1.WriteMessages(context.Background(), mess); err != nil {
				logger.ErrorLogger.Println(err)
			}
			logger.InfoLogger.Println("message sent to kafka writer")

		}()

		time.Sleep(10 * time.Millisecond)
	}
}

//stream3
func (s Stream) Stream3(stream pb.Stream_Stream3Server) error {

	defer s.kafkaWriter1.Close()

	for {

		body, err := stream.Recv()
		if err != nil {
			logger.ErrorLogger.Println(err)
			return err
		}

		go func() {

			mess := kafka.Message{
				Key:   []byte("stream1"),
				Value: body.Message,
				Time:  time.Now(),
			}

			if err := s.kafkaWriter1.WriteMessages(context.Background(), mess); err != nil {
				logger.ErrorLogger.Println(err)
			}
			logger.InfoLogger.Println("message sent to kafka writer")

		}()

		time.Sleep(10 * time.Millisecond)
	}
}
