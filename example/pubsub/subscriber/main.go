package main

import (
	"context"
	"github.com/cheshir/go-mq/v2"
	"gopkg.in/yaml.v1"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var externalConfigSubscriber = `rabbitmq:
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
queues:
  - name: "queue_name"
    exchange: "logs"
    routing_key: "pub.#"
    options:
      durable: true
consumers:
  - name: "consumer_name"
    queue: "queue_name"
    workers: 1
`

func main() {
	var config mq.Config
	err := yaml.Unmarshal([]byte(externalConfigSubscriber), &config)
	if err != nil {
		log.Fatal("Failed to read config", err)
	}
	ctx := contextWithCtrlC(context.Background())

	messageQueue, err := mq.New(config)
	if err != nil {
		log.Fatal("Failed to initialize message queue manager", err)
	}
	defer messageQueue.Close()

	go func() {
		for err := range messageQueue.Error() {
			log.Fatal("Caught error from message queue: ", err)
		}
	}()

	err = messageQueue.SetConsumerHandler("consumer_name", func(message mq.Message) {
		const routingKeyWidth = 30
		const messageBodyWidth = 60
		log.Printf("[>>>] %-*s | %-*s | To exit press CTRL+C\n",
			routingKeyWidth, message.RoutingKey(),
			messageBodyWidth, message.Body(),
		)

		message.Ack(false)
	})
	if err != nil {
		log.Fatalf("Failed to set handler to consumer `%s`: %v", "consumer_name", err)
	}
	<-ctx.Done()
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
