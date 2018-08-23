package queue

import (
	"encoding/json"
)

const (
	QUEUE_MESSAGE_CODE_ENQUEUE   = "enqueue"
	QUEUE_MESSAGE_CODE_REENQUEUE = "reenqueue"
)

// QueueMessage is the struct to send data over the queue
type QueueMessage struct {
	Data interface{} `json:"data"`
	Code string      `json:"code"`
}

// EncodeMessageQueue JSON encodes the EncodeXLSXMessageQueue to be sent over the IQueue
func EncodeMessageQueue(data *QueueMessage) ([]byte, error) {
	return json.Marshal(data)
}

// DecodeMessageQueue decodes the raw JSON into a *structs.XLSXQueueMessage. To be used by the method that pulls xlsx messages from the queue.
func DecodeMessageQueue(data []byte) (*QueueMessage, error) {
	var ret QueueMessage

	err := json.Unmarshal(data, &ret)
	if err != nil {
		return nil, err
	}

	return &ret, nil
}
