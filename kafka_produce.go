package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
)

var logger = log.New(os.Stderr, "[TEST]", log.LstdFlags)

func main() {
	sarama.Logger = logger

	config := sarama.NewConfig()
	config.ClientID = "newsDataSource"
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	msg := &sarama.ProducerMessage{}
	msg.Topic = "test"
	msg.Partition = int32(-1)
	msg.Key = sarama.StringEncoder("key")
	msg.Value = sarama.ByteEncoder("hello")

	producer, err := sarama.NewSyncProducer(strings.Split("192.168.127.226:9092", ","), config)
	if err != nil {
		logger.Printf("Failed to produce message :%s", err)
		os.Exit(500)
	}

	defer producer.Close()

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		logger.Printf("Failed to produce message :%s", err)
	}
	logger.Printf("partition:%d, offset: %d\n", partition, offset)
}
