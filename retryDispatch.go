package rmqretr

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/BetaLixT/usago"
	"github.com/betalixt/gorr"
	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type RetryDispatch struct {
	// Event queue
	eventQueue chan retryMessage

	// Rabbit MQ
	chnlManager *usago.ChannelManager
	channelCtx  *usago.ChannelContext
	cnfrmch     *chan amqp091.Confirmation

	// Message tracking
	messageCount      int
	messageCountMutex sync.Mutex

	// ack pending
	pendingMutex sync.Mutex
	pendingsRaw  map[uint64]retryMessage

	// Common
	closing     bool
	closed      bool
	lgr         *zap.Logger
	retryExName string
	retryExType string
	wg          sync.WaitGroup
	tracer      ITracer
}

func NewNotifDispatch(
	chnlManager *usago.ChannelManager,
	lgr *zap.Logger,
	tracer ITracer,
	retryExName string,
	retryExType string,
) *RetryDispatch {

	disp := &RetryDispatch{
		eventQueue:   make(chan retryMessage, 1000),
		chnlManager:  chnlManager,
		messageCount: 0,
		pendingsRaw:  map[uint64]retryMessage{},
		closing:      false,
		closed:       false,
		lgr:          lgr,
		retryExName:  retryExName,
		retryExType:  retryExType,
		tracer:       tracer,
	}

	// - setting up channel
	chnlBuilder := usago.NewChannelBuilder().WithExchange(
		retryExName,
		retryExType,
		true,
		false,
		false,
		false,
		nil,
	).WithConfirms(true)

	channelCtx, err := chnlManager.NewChannel(*chnlBuilder)
	if err != nil {
		lgr.Error(
			"failed to build channel",
			zap.Error(err),
		)
		panic(err)
	}
	disp.channelCtx = channelCtx
	cnfrmch, err := channelCtx.GetConfirmsChannel()
	if err != nil {
		lgr.Error(
			"failed to fetch confirms channel",
			zap.Error(err),
		)
		panic(err)
	}
	disp.cnfrmch = &cnfrmch

	// - dispatching chan workers
	disp.wg.Add(2)
	go func() {
		disp.confirmHandler(*disp.cnfrmch)
		disp.wg.Done()
	}()
	go func() {
		disp.processQueue()
		disp.wg.Done()
	}()

	return disp
}

func (disp *RetryDispatch) Close() {
	disp.closing = true

	disp.lgr.Info("closing publish observer...")
	if disp.pendingMessages() != 0 {
		disp.lgr.Info(
			"waiting for pending messages",
		)
	}
	prevCount := disp.pendingMessages()
	sameCountRetr := 0

	for disp.pendingMessages() != 0 && sameCountRetr < 10 {
		time.Sleep(100 * time.Millisecond)
		curr := disp.pendingMessages()
		if prevCount == curr {
			sameCountRetr++
		} else {
			sameCountRetr = 0
			prevCount = curr
		}
	}
	close(disp.eventQueue)
	disp.closed = true
	disp.chnlManager.Discard(disp.channelCtx)
	disp.wg.Wait()
}

// - Event channel handling
func (disp *RetryDispatch) DispatchEventNotification(
	ctx context.Context,
	queueName string,
	msg amqp091.Delivery,
	maxRetries int,
	duration time.Duration,
) error {

	if disp.closing {
		return gorr.NewError(
			gorr.ErrorCode{
				Code:    12000,
				Message: "DispatchChannelClosed",
			},
			500,
			"",
		)
	}
	retries := msg.Headers[RETRIES_HEADER_KEY]
	retrParsed := 0
	retrParsed, ok := retries.(int)
	if ok {
		if retrParsed >= maxRetries {
			disp.lgr.Error("failed after max retries")
			return fmt.Errorf("failed after max retries")
		}
		retrParsed++
	}

	ver, tid, pid, rid, flg := disp.tracer.ExtractTraceInfo(ctx)
	disp.messageQueued()
	disp.eventQueue <- retryMessage{
		QueueName:       queueName,
		Retries:         retrParsed,
		WaitDuration:    duration,
		ContentType:     msg.ContentType,
		Body:            msg.Body,
		Ver:             ver,
		Tid:             tid,
		Pid:             pid,
		Rid:             rid,
		Flg:             flg,
		Tracepart:       "000",
		InternalRetries: 0,
	}
	return nil
}

func (disp *RetryDispatch) retryEventNotification(
	evnt retryMessage,
) {
	evnt.InternalRetries += 1
	disp.eventQueue <- evnt
}

func (disp *RetryDispatch) processQueue() {
	active := true
	var evnt retryMessage
	for active {
		evnt, active = <-disp.eventQueue
		if active {
			disp.publishEvent(evnt)
		}
	}
}

func (disp *RetryDispatch) publishEvent(msg retryMessage) error {	

	// TODO implement tracepart
	msg.RequestStartTime = time.Now()
	sqno, err := disp.channelCtx.Publish(
		disp.retryExName,
		msg.QueueName,
		true,
		false,
		amqp091.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
			Expiration:  strconv.Itoa(int(msg.WaitDuration.Seconds()) * msg.Retries),
			Headers: amqp091.Table{
				"traceparent": fmt.Sprintf(
					"%s-%s-%s-%s",
					msg.Ver,
					msg.Tid,
					msg.Pid,
					msg.Flg,
				),
				RETRIES_HEADER_KEY: msg.Retries,
			},
		},
	)
	if err != nil {
		disp.lgr.Error("Failed to publish message", zap.Error(err))
		disp.retryEventNotification(msg)
	} else {
		disp.setPending(sqno, msg)
	}
	return nil
}

func (b *RetryDispatch) confirmHandler(confirms chan amqp091.Confirmation) {
	open := true
	var confirmed amqp091.Confirmation
	for open {
		confirmed, open = <-confirms
		if confirmed.DeliveryTag > 0 {
			if confirmed.Ack {
				// TODO: error handling just incase
				conf := b.getPending(confirmed.DeliveryTag)
				b.tracer.TraceDependencyWithIds(
					conf.Tid,
					conf.Rid,
					"",
					"RabbitMQ",
					b.retryExName,
					"notify",
					true,
					conf.RequestStartTime,
					time.Now(),
					map[string]string{},
				)
				b.lgr.Info(
					"confirmed retry delivery",
					zap.String("tid", conf.Tid),
					zap.String("pid", conf.Pid),
					zap.String("rid", conf.Rid),
					zap.String("tpart", conf.Tracepart),
				)
				b.messageDispatched()
			} else {
				failed := b.getPending(confirmed.DeliveryTag)
				b.tracer.TraceDependencyWithIds(
					failed.Tid,
					failed.Rid,
					"",
					"RabbitMQ",
					b.retryExName,
					"notify",
					false,
					failed.RequestStartTime,
					time.Now(),
					map[string]string{},
				)
				b.lgr.Warn(
					"failed notification delivery",
					zap.Int("retry", failed.InternalRetries),
					zap.String("tid", failed.Tid),
					zap.String("pid", failed.Pid),
					zap.String("rid", failed.Rid),
					zap.String("tpart", failed.Tracepart),
				)

				// the channel may be filled
				go func() {
					b.retryEventNotification(failed)
				}()
			}
			b.delPending(confirmed.DeliveryTag)
		}
	}
	b.lgr.Debug("confirms channel has closed")
}

// - Pending ack messages
func (b *RetryDispatch) getPending(key uint64) retryMessage {
	b.pendingMutex.Lock()
	v := b.pendingsRaw[key]
	b.pendingMutex.Unlock()
	return v
}

func (b *RetryDispatch) setPending(key uint64, evnt retryMessage) {
	b.pendingMutex.Lock()
	b.pendingsRaw[key] = evnt
	b.pendingMutex.Unlock()
}

func (b *RetryDispatch) delPending(key uint64) {
	b.pendingMutex.Lock()
	delete(b.pendingsRaw, key)
	b.pendingMutex.Unlock()
}

// - Message tracking handler
func (disp *RetryDispatch) messageQueued() {
	disp.messageCountMutex.Lock()
	disp.messageCount += 1
	disp.messageCountMutex.Unlock()
}

func (disp *RetryDispatch) messageDispatched() {
	disp.messageCountMutex.Lock()
	disp.messageCount -= 1
	disp.messageCountMutex.Unlock()
}

func (disp *RetryDispatch) pendingMessages() int {
	disp.messageCountMutex.Lock()
	pending := disp.messageCount
	disp.messageCountMutex.Unlock()
	return pending
}
