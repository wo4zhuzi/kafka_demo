package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
)

var host = "localhost:9092"

func main()  {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{host},
		GroupID:   "one",
		Topic:     "test1",
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	ctx := context.Background()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Fatal("failed to commit messages:", err)
		}
	}
}