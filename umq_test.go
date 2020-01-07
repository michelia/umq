package umq

import (
	"fmt"
	"testing"
)

func TestUmq(t *testing.T) {
	c := Config{
		BrokerUrl: "tcp://127.0.0.1:8883",
	}
	mqc := New(c)
	mqc.OnlineCallback = func(resumed bool) {
		fmt.Println("mqc OnlineCallback")
	}
	mqc.OfflineCallback = func() {
		fmt.Println("mqc OfflineCallback")
	}
	mqc.SetMessageCallback(func(msg *Message) {
		switch {
		case Match(msg.Topic, "topic_a/+"):
			// go msgHandleTopic1(msg.Topic, msg.Payload)
		case Match(msg.Topic, "topic_b/+"):
			// go msgHandleTopic2(msg.Topic, msg.Payload)
		}
	})
	if err := mqc.Start(); err != nil {
		panic("start error")
	}
	if err := mqc.Publish(&Message{
		Topic:   "topic_a/abc",
		QOS:     1,
		Payload: []byte("payload"),
	}); err != nil {
		panic("Publish error")
	}
}
