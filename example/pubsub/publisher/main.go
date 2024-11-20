package main

import (
	"context"
	"fmt"
	"github.com/cheshir/go-mq/v2"
	"gopkg.in/yaml.v1"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var externalConfigPublisher = `rabbitmq:
dsn: "amqp://guest:guest@localhost:5672/"
reconnect_delay: 1s
exchanges:
  - name: "logs"
    type: "fanout"
    options:
      durable: true
      auto-deleted: false
      internal: false
      no-wait: false
producers:
  - name: "async_producer"
    exchange: "logs"
    routing_key: "pub.async_producer"
    options:
      content_type: "text/plain"
  - name: "sync_producer"
    exchange: "logs"
    routing_key: "pub.sync_producer"
    sync: true
    options:
      content_type: "text/plain"
`

func main() {
	var config mq.Config
	err := yaml.Unmarshal([]byte(externalConfigPublisher), &config)
	if err != nil {
		log.Fatal("Failed to read config", err)
	}
	ctx := contextWithCtrlC(context.Background())
	messageQueue, err := mq.New(config)
	if err != nil {
		log.Fatal("Failed to initialize message queue manager", err)
	}
	defer messageQueue.Close()

	body := bodyFrom(os.Args)

	go func() {
		for err := range messageQueue.Error() {
			log.Fatal("Caught error from message queue: ", err)
		}
	}()

	go func() {
		producer, err := messageQueue.SyncProducer("sync_producer")
		if err != nil {
			log.Fatal("Failed to get sync producer: ", err)
		}
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
	SyncLoop:
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				log.Print("stopping SYNC")
				break SyncLoop
			case <-ticker.C:
				msg := fmt.Sprintf("SYNC: %s #%d", body, i)
				err = producer.Produce([]byte(msg))
				if err != nil {
					log.Fatal("Failed to send message from sync producer")
				}
				log.Printf(" [<<<] \"%s\" \t| To exit press CTRL+C\n", msg)
				rk := fmt.Sprintf("pub.sync_producer.rk_%d", i)
				err = producer.Publish(rk, []byte(msg))
			}
		}
		log.Print("DONE - Exit SYNC")
	}()

	producer, err := messageQueue.AsyncProducer("async_producer")
	if err != nil {
		log.Fatal("Failed to get async producer: ", err)
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
AsyncLoop:
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			log.Print("stopping ASYNC")
			break AsyncLoop
		case <-ticker.C:
			msg := fmt.Sprintf("ASYNC: %s #%d", body, i)
			producer.Produce([]byte(msg))
			log.Printf(" [<<<] \"%s\" \t| To exit press CTRL+C\n", msg)

			rk := fmt.Sprintf("pub.async_producer.rk_%d", i)
			producer.Publish(rk, []byte(msg))
		}
	}
	log.Print("DONE - Exit AYNC")
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}

func contextWithCtrlC(ctx context.Context) context.Context {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-signalChan
		log.Printf("Received signal: %s", sig)
		cancel()
	}()
	return ctxWithCancel
}
