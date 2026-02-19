package main

import (
	"fmt"
	"log"
	"time"

	simple_kafka "github.com/sikalabsx/simple-kafka-go/pkg/simple_kafka"
)

func main() {
	sk := simple_kafka.SimpleKafkaNewWriterFromEnv()

	i := 0
	for {
		key := []byte(fmt.Sprintf("%d", i))
		value := []byte(fmt.Sprintf("Hello_%d", i))
		err := sk.Produce(key, value)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Produced message with key=%s value=%s\n", key, value)
		i++
		time.Sleep(1 * time.Second)
	}
}
