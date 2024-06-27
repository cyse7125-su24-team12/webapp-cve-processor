package main

import (
	"fmt"
	"log"
	"os"

	"github.com/IBM/sarama"
)

var (
	kafkaHost     = os.Getenv("KAFKA_HOST")
	kafkaUser     = os.Getenv("KAFKA_USER")
	kafkaPassword = os.Getenv("KAFKA_PASSWORD")
	topicName   = os.Getenv("TOPIC_NAME")
)

func checkTopicExists(admin sarama.ClusterAdmin, topic string) (bool, error) {
    topics, err := admin.ListTopics()
    if err != nil {
        return false, err
    }
    _, exists := topics[topic]
    return exists, nil
}

func SendMessageToKafka(topic string, jsonBytes []byte, producer sarama.SyncProducer){
	// Publish a message to the Kafka topic
    message := &sarama.ProducerMessage{
        Topic: topic,
        Value: sarama.ByteEncoder(jsonBytes),
    }
    partition, offset, err := producer.SendMessage(message)
    if err != nil {
        log.Fatalf("Failed to send message: %v", err)
    }
    log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}

func SetupKafka() (string, sarama.SyncProducer, error) {
	// Kafka broker address
    brokerList := []string{kafkaHost}
	topic := topicName

    // SASL/PLAIN authentication configuration
    config := sarama.NewConfig()
    config.Net.SASL.Enable = true
    config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
    config.Net.SASL.User = kafkaUser
    config.Net.SASL.Password = kafkaPassword
    config.Producer.RequiredAcks = sarama.WaitForAll
    config.Producer.Return.Successes = true
    config.Producer.MaxMessageBytes = 600000000
    config.Net.MaxOpenRequests = 1
    config.Producer.Idempotent = true
    config.Producer.Compression = sarama.CompressionSnappy

	// Enable debug logging
    sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	// Check if the topic exists
	admin, err := sarama.NewClusterAdmin(brokerList, config)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create Kafka cluster admin: %w", err)
	}
	defer admin.Close()

	topicExists, err := checkTopicExists(admin, topic)
	if err != nil {
		return "", nil, fmt.Errorf("failed to check if topic exists: %w", err)
	}
	if !topicExists {
		return "", nil, fmt.Errorf("topic does not exist")
	}

	// Create a new Kafka producer
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return topic, producer, nil
}