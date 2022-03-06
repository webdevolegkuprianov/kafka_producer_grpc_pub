package main

import (
	"github.com/webdevolegkuprianov/kafka_producer_grpc/app/producer"
	logger "github.com/webdevolegkuprianov/kafka_producer_grpc/app/producer/logger"
	"github.com/webdevolegkuprianov/kafka_producer_grpc/model"
)

func main() {

	config, err := model.NewConfig()
	if err != nil {
		logger.ErrorLogger.Println(err)
	}

	if err := producer.Start(config); err != nil {
		logger.ErrorLogger.Println(err)
	}

}
