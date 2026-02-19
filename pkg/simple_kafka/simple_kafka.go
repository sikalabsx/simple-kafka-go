package simple_kafka

import (
	"context"
	"os"

	"github.com/segmentio/kafka-go"
)

type SimpleKafka struct {
	reader *kafka.Reader
	writer *kafka.Writer
}

func SimpleKafkaNewReader(brokers []string, topic, groupID string) SimpleKafka {
	return SimpleKafka{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
	}
}

func SimpleKafkaNewReaderFromEnv() SimpleKafka {
	brokers := []string{os.Getenv("SIMPLE_KAFKA_BROKER")}
	topic := os.Getenv("SIMPLE_KAFKA_TOPIC")
	groupID := os.Getenv("SIMPLE_KAFKA_GROUP_ID")
	return SimpleKafkaNewReader(brokers, topic, groupID)
}

func SimpleKafkaNewWriter(brokers []string, topic string) SimpleKafka {
	return SimpleKafka{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: brokers,
			Topic:   topic,
		}),
	}
}

func SimpleKafkaNewWriterFromEnv() SimpleKafka {
	brokers := []string{os.Getenv("SIMPLE_KAFKA_BROKER")}
	topic := os.Getenv("SIMPLE_KAFKA_TOPIC")
	return SimpleKafkaNewWriter(brokers, topic)
}

func (sk *SimpleKafka) Consume() (kafka.Message, error) {
	msg, err := sk.reader.ReadMessage(context.Background())
	return msg, err
}

func (sk *SimpleKafka) Produce(key, value []byte) error {
	msg := kafka.Message{
		Key:   key,
		Value: value,
	}
	return sk.writer.WriteMessages(context.Background(), msg)
}
