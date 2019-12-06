package umq

import (
	"testing"
	"time"

	"github.com/michelia/ulog"
)

func TestUmq(t *testing.T) {
	slog := ulog.NewConsole()
	c := Config{
		BrokerAddr:     "tcp://127.0.0.1:8883",
		UserName:       "foo",
		Password:       "bar",
		ConnectTimeout: 1000,
		PublishTimeout: 100,
	}
	mq := New(slog, c)
	mq.SetClientID(func() string {
		return "testid-send"
	})
	mq.AddSubscrAdd("test/123/1", 2, func(cli Client, msg Message) {
		slog.Info().Msg("收到: " + string(msg.Payload()))
	})
	err := mq.Connect()
	t.Log(err)
	err = mq.Publish(slog, "test/123/1", 2, false, "test-foo-bar")
	t.Log(err)
	time.Sleep(time.Second * 3)
}
