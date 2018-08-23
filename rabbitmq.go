package queue

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

/************************************************************************************************
 ** RabbitMQ ** IQueue's implementation *********************************************************
 ************************************************************************************************
 */

type RabbitMQQueue struct {
	Protocol string `json:"protocol"`
	Hostname string `json:"hostname"`
	Username string `json:"username"`
	Password string `json:"password"`
	Port     int16  `json:"port"`
}

func (r RabbitMQQueue) buildConnectionPath() string {
	return fmt.Sprintf("%s://%s:%s@%s:%d/", r.Protocol, r.Username, r.Password, r.Hostname, r.Port)
}

type RabbitMQ struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queues    map[string]amqp.Queue
	queueData RabbitMQQueue
}

// NewRabbitMQ creates and returns a *RabbitMQ instance
func NewRabbitMQ(queueData RabbitMQQueue) (*RabbitMQ, error) {
	rabbitmq := &RabbitMQ{}
	err := rabbitmq.initialize(queueData)
	if err != nil {
		return nil, err
	}

	return rabbitmq, nil
}

func (st *RabbitMQ) initialize(queueData RabbitMQQueue) error {
	st.queueData = queueData

	st.queues = make(map[string]amqp.Queue)

	// initialize connection & channel
	err := st.initializeConnectionAndChannel()
	if err != nil {
		return err
	}

	return nil
}

// initializeConnectionAndChannel initializes the RabbitMQ connection && channel
func (st *RabbitMQ) initializeConnectionAndChannel() error {
	// build the connection query
	rabbitMqConnection := st.queueData.buildConnectionPath()

	var err error
	st.conn, err = amqp.Dial(rabbitMqConnection)
	if err != nil {
		return err
	}

	// create the connection
	st.channel, err = st.conn.Channel()
	if err != nil {
		return err
	}

	// Controls how many messages or how many bytes the server will try to keep on
	// the network for consumers before receiving delivery acks.
	err = st.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return err
	}

	return nil
}

// DeclareQueue declares && initializes the queue
func (st *RabbitMQ) DeclareQueue(queueName string) error {
	// declare the queue
	queue, err := st.channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		st.queues[queueName] = queue
	}

	return err
}

// Close closes the RabbitMQ connection
func (st *RabbitMQ) Close() error {
	return st.conn.Close()
}

func (st *RabbitMQ) PublishString(queueName string, data string) error {
	// try to publish the data over the channel
	err := st.publishString(queueName, data)

	if err != nil {
		switch err.(type) {
		case *amqp.Error:
			// channel/connection is not open, try to open it again
			amqpError := err.(*amqp.Error)
			if amqpError.Code == amqp.ChannelError {
				err2 := st.initializeConnectionAndChannel()

				if err2 == nil {
					// try to publish again the data over the channel
					err = st.publishString(queueName, data)
				}
			}
		}
	}

	return err
}

// to be used by *RabbitMQ.PublishString(...)
func (st *RabbitMQ) publishString(queueName string, data string) error {
	err := st.channel.Publish(
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(string(data)),
		})

	return err
}

// TODO ::: Check this && add comments !!!
func (st *RabbitMQ) Consume(queueName string, cFunc IConsumer) error {

	// get the messages channel
	msgs, err := st.getConsumerMsgs(queueName)

	if err != nil {

		switch err.(type) {
		// Check if the error is *amqp.Error(code 504), then restart the connection/channel
		case *amqp.Error:
			// channel/connection is not open, try to open it again

			amqpError := err.(*amqp.Error)

			// code 504, try to restart the connection/channel
			if amqpError.Code == amqp.ChannelError {
				// try to restart the connection with RabbitMQ
				err2 := st.initializeConnectionAndChannel()
				if err2 != nil {
					return err
				}

				// try to get again the messages channel
				msgs, err2 = st.getConsumerMsgs(queueName)

				if err2 != nil {
					return err
				}
			}

		default:
			// return the error
			return err
		}

	}

	forever := make(chan bool)

	go func(ch chan bool) {
		for d := range msgs {
			if err := cFunc.ConsumerFunc(d.Body); err != nil {
				// TODO ::: send the error back
				// TODO ::: Set a max number of possible attempts and Ack() the message once the number is reached, also call a callbackError func to let the client know

				log.Println(err)

				// if we don't Ack the message, this goroutine will not receive any new message. Research about the implications of d.Reject()
				d.Ack(false)
			} else {
				// tell RabbitMQ that the message was received and processed, so it is free to remove it from the queue
				d.Ack(false)
			}
		}

		ch <- true
	}(forever)

	<-forever

	log.Println("The IQueue connection/channel died")

	return nil
}

// getConsumerMsgs returns the messages (<- chan aqmp.Delivery) to consume. This func should be used ONLY by *RabbitMQ.Consume(...)
func (st *RabbitMQ) getConsumerMsgs(queueName string) (<-chan amqp.Delivery, error) {
	return st.channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
}
