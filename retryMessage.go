package rmqretr

import (
	"time"
)

type retryMessage struct {
	QueueName    string
	Retries      int32
	WaitDuration time.Duration
	ContentType  string
	Body         []byte
	RoutingKey   string

	// Trace inco
	Ver              string
	Tid              string
	Pid              string
	Rid              string
	Flg              string
	Tracepart        string
	RequestStartTime time.Time
	InternalRetries  int
}
