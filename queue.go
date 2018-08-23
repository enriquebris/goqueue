package goqueue

type IQueue interface {
	PublishString(queueName string, data string) error
	Consume(queueName string, cFunc IConsumer) error
	Close() error
}

// function to be used in IQueue.Consume(). The queue's message will be erased once this function return nil.
type ConsumerFunc func(data []byte) error

type IConsumer interface {
	ConsumerFunc(data []byte) error
}
