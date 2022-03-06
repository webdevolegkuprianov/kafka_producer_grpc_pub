package model

import (
	"io/ioutil"
	"path/filepath"

	logger "github.com/webdevolegkuprianov/kafka_producer_grpc/app/producer/logger"
	"gopkg.in/yaml.v2"
)

//config yaml struct
type Service struct {
	KafkaEcoSystem string `yaml:"kafka_eco_system"`
	Spec           struct {
		Ports struct {
			GrpcServer struct {
				Addr string `yaml:"bind_addr"`
			} `yaml:"grpc_server"`
		} `yaml:"ports"`
		KafkaConf struct {
			Brokers struct {
				Broker1 string `yaml:"broker1"`
			} `yaml:"brokers"`
			Topics struct {
				Topic1 string `yaml:"topic1"`
			} `yaml:"topics"`
			Keys struct {
				Key1 string `yaml:"key1"`
			} `yaml:"keys"`
			KafkaClientId string `yaml:"kafka_client_id"`
		} `yaml:"kafka_conf"`
		ProducerConf struct {
			Async         bool `yaml:"async"`
			MaxAttemps    int  `yaml:"max_attempts"`
			BatchSize     int  `yaml:"batch_size"`
			BatchBytes    int  `yaml:"batch_bytes"`
			BatchtTimeout int  `yaml:"batch_timeout"`
			WriteTimeout  int  `yaml:"write_timeout"`
			ReadTimeout   int  `yaml:"read_timeout"`
			RequiredAcks  int  `yaml:"required_acks"`
		} `yaml:"producer"`
		Dialer struct {
			Timeout int `yaml:"timeout"`
		} `yaml:"dialer"`
		Logs struct {
			Path string `yaml:"path"`
		} `yaml:"logs"`
	} `yaml:"spec"`
}

//new config
func NewConfig() (*Service, error) {

	var service *Service

	f, err := filepath.Abs("/root/config/kafka.yaml")
	if err != nil {
		logger.ErrorLogger.Println(err)
		return nil, err
	}

	y, err := ioutil.ReadFile(f)
	if err != nil {
		logger.ErrorLogger.Println(err)
		return nil, err
	}

	if err := yaml.Unmarshal(y, &service); err != nil {
		logger.ErrorLogger.Println(err)
		return nil, err
	}

	return service, nil

}
