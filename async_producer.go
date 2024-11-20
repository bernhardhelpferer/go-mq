package mq

import (
	"sync"

	"github.com/NeowayLabs/wabbit"
)

// AsyncProducer describes available methods for producer.
// This kind of producer is asynchronous.
// All occurred errors will be accessible with MQ.Error().
type AsyncProducer interface {
	// Produce sends message to broker. Returns immediately.
	Produce(data []byte)
	// Publish sends message to broker using the given routing key.  Returns immediately.
	Publish(route string, message []byte)
}
type asyncProducerMessage struct {
	data       []byte
	routingKey string
}
type asyncProducer struct {
	sync.Mutex // Protect channel during posting and reconnect.
	workerStatus

	channel         wabbit.Channel
	errorChannel    chan<- error
	exchange        string
	options         wabbit.Option
	publishChannel  chan *asyncProducerMessage
	routingKey      string
	shutdownChannel chan chan struct{}
}

func newAsyncProducer(channel wabbit.Channel, errorChannel chan<- error, config ProducerConfig) *asyncProducer {
	return &asyncProducer{
		channel:         channel,
		errorChannel:    errorChannel,
		exchange:        config.Exchange,
		options:         wabbit.Option(config.Options),
		publishChannel:  make(chan *asyncProducerMessage, config.BufferSize),
		routingKey:      config.RoutingKey,
		shutdownChannel: make(chan chan struct{}),
	}
}

func (producer *asyncProducer) init() {
	go producer.worker()
}

func (producer *asyncProducer) worker() {
	producer.markAsRunning()

	for {
		select {
		case message := <-producer.publishChannel:
			err := producer.produce(message)
			if err != nil {
				producer.errorChannel <- err
				// TODO Resend message.
			}
		case done := <-producer.shutdownChannel:
			// TODO It is necessary to guarantee the message delivery order.
			producer.closeChannel()
			close(done)

			return
		}
	}
}

// Method safely sets new RMQ channel.
func (producer *asyncProducer) setChannel(channel wabbit.Channel) {
	producer.Lock()
	producer.channel = channel
	producer.Unlock()
}

// Close producer's channel.
func (producer *asyncProducer) closeChannel() {
	producer.Lock()
	if err := producer.channel.Close(); err != nil {
		producer.errorChannel <- err
	}
	producer.Unlock()
}

func (producer *asyncProducer) Produce(message []byte) {
	producer.publishChannel <- &asyncProducerMessage{
		data:       message,
		routingKey: producer.routingKey,
	}
}

func (producer *asyncProducer) produce(message *asyncProducerMessage) error {
	producer.Lock()
	defer producer.Unlock()

	return producer.channel.Publish(producer.exchange, message.routingKey, message.data, producer.options)
}

func (producer *asyncProducer) Publish(route string, message []byte) {
	producer.publishChannel <- &asyncProducerMessage{
		data:       message,
		routingKey: route,
	}
}

// Stops the worker if it is running.
func (producer *asyncProducer) Stop() {
	if producer.markAsStoppedIfCan() {
		done := make(chan struct{})
		producer.shutdownChannel <- done
		<-done
	}
}
