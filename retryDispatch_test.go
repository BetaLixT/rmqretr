package rmqretr

import (
	"context"
	"testing"
	"time"

	"github.com/BetaLixT/usago"
	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type MockTracer struct {
}

var _ ITracer = (*MockTracer)(nil)

func (_ *MockTracer) TraceDependencyWithIds(
	tid string,
	rid string,
	spanId string,
	dependencyType string,
	serviceName string,
	commandName string,
	success bool,
	startTimestamp time.Time,
	eventTimestamp time.Time,
	fields map[string]string,
) {

}

func (_ *MockTracer) ExtractTraceInfo(
	ctx context.Context,
) (ver, tid, pid, rid, flg string) {
	return "", "", "", "", ""
}

func GetRetryDispatch() *RetryDispatch {
	lgr, _ := zap.NewDevelopment()
	usago := usago.NewChannelManager(
		"amqp://guest:guest@localhost:5672/",
		lgr,
	)
	return NewRetryDispatch(
		usago,
		lgr,
		&MockTracer{},
		"retryex",
		"retryex.waitq",
		"retryex.waitq.dlx",
	)
}

func TestDispatchEventNotification_SingleRetry(t *testing.T) {
	lgr, _ := zap.NewDevelopment()
	usg := usago.NewChannelManager(
		"amqp://guest:guest@localhost:5672/",
		lgr,
	)
	bldr := usago.NewChannelBuilder()
	qname := "temporaryq3425"
	bldr.WithExchange(
		"retryex.waitq.dlx",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	).WithQueue(
		qname,
		true,
		true,
		false,
		false,
		nil,
	).WithQueueBinding(
		qname,
		qname,
		"retryex.waitq.dlx",
		false,
		nil,
	)

	ch, err := usg.NewChannel(*bldr)
	if err != nil {
		lgr.Error(
			"failed to create channel",
			zap.Error(err),
		)
		t.FailNow()
	}

	body := "Hello World!"
	ch.Publish(
		"",
		qname,
		true,
		false,
		amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	cnsChan, err := ch.RegisterConsumer(
	  qname,
	  "",
	  false,
	  false,
	  false,
	  false,
	  nil,
	)
	if err != nil {
		lgr.Error(
			"failed to create channel",
			zap.Error(err),
		)
		t.FailNow()
	}
	rdisp := GetRetryDispatch()
	for i := 0; i < 2; i++ {
	  lgr.Info("waiting...")
	  msg := <- cnsChan
	  msg.Ack(false)
	  if i == 0 {
	    rdisp.DispatchRetry(
	      context.Background(),
	      qname,
	      msg,
	      3,
	      time.Second,
	    )
	  }
	  lgr.Info("recv", zap.ByteString("body", msg.Body))
	}
}

func TestDispatchEventNotification_ThreeRetry(t *testing.T) {
	lgr, _ := zap.NewDevelopment()
	usg := usago.NewChannelManager(
		"amqp://guest:guest@localhost:5672/",
		lgr,
	)
	bldr := usago.NewChannelBuilder()
	qname := "temporaryq34432"
	bldr.WithExchange(
		"retryex.waitq.dlx",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	).WithQueue(
		qname,
		true,
		true,
		false,
		false,
		nil,
	).WithQueueBinding(
		qname,
		qname,
		"retryex.waitq.dlx",
		false,
		nil,
	)

	ch, err := usg.NewChannel(*bldr)
	if err != nil {
		lgr.Error(
			"failed to create channel",
			zap.Error(err),
		)
		t.FailNow()
	}

	body := "Hello World!"
	ch.Publish(
		"",
		qname,
		true,
		false,
		amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	cnsChan, err := ch.RegisterConsumer(
	  qname,
	  "",
	  false,
	  false,
	  false,
	  false,
	  nil,
	)
	if err != nil {
		lgr.Error(
			"failed to create channel",
			zap.Error(err),
		)
		t.FailNow()
	}
	rdisp := GetRetryDispatch()
	for i := 0; i < 4; i++ {
	  lgr.Info("waiting...")
	  msg := <- cnsChan
	  msg.Ack(false)
	  if i < 3 {
	    rdisp.DispatchRetry(
	      context.Background(),
	      qname,
	      msg,
	      3,
	      time.Second,
	    )
	  }
	  lgr.Info("recv", zap.ByteString("body", msg.Body))
	}
}
