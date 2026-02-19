package main

import (
	"log"

	simple_kafka "github.com/sikalabsx/simple-kafka-go/pkg/simple_kafka"
)

func main() {
	sk := simple_kafka.SimpleKafkaNewReader(
		[]string{"localhost:9092"},
		"example-topic",
		"example-group",
	)

	for {
		msg, err := sk.Consume()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Consumed message with key=%s value=%s\n", msg.Key, msg.Value)
	}
}
