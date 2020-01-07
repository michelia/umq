package umq

import (
	"strings"
	"time"

	mqc "github.com/256dpi/gomqtt/client"
	"github.com/256dpi/gomqtt/packet"
)

// Match checks if the Topic Name matches the Topic Filter.
// topicName   topic/foo
// topicFilter topic/+
func Match(topicName, topicFilter string) bool {
	// Tokenize the Topic Name.
	nameTokens := strings.Split(topicName, "/")
	nameTokensLen := len(nameTokens)

	// Tolenize the Topic Filter.
	filterTokens := strings.Split(topicFilter, "/")

	for i, t := range filterTokens {
		switch t {
		case "#":
			return i != 0 || !strings.HasPrefix(nameTokens[0], "$")
		case "+":
			if i == 0 && strings.HasPrefix(nameTokens[0], "$") {
				return false
			}

			if nameTokensLen <= i {
				return false
			}
		default:
			if nameTokensLen <= i || t != nameTokens[i] {
				return false
			}
		}
	}

	return len(filterTokens) == nameTokensLen
}

// Config 构建mq需要的配置
type Config struct {
	ClientID            string
	BrokerUrl           string        // "tcp://user:password@host:port"
	KeepAlive           time.Duration // 单位s 默认是30s
	DisableCleanSession bool
	PublishTimeout      time.Duration // 单位s 默认是1s
	MaxReconnectDelay   time.Duration // 单位s 默认是10s
}

type Message = packet.Message
type QOS = packet.QOS

type MessageHandler func(topic string, payload []byte)

type MQC struct {
	*mqc.Service
	cfg            *mqc.Config
	Subscribes     map[string]int
	PublishTimeout time.Duration
}

func (m *MQC) Start() error {
	m.Service.Start(m.cfg)
	var err error
	for topic, qos := range m.Subscribes {
		err = m.Service.Subscribe(topic, QOS(qos)).Wait(5 * time.Second)
	}
	return err
}

func (m *MQC) Stop() {
	m.Service.Stop(true)
}
func (m *MQC) Publish(msg *Message) error {
	return m.Service.PublishMessage(msg).Wait(m.PublishTimeout)
}

// SetMessageCallback 在 Start 之前
// func(msg *umq.Message) {
// 	switch {
// 	case umq.Match(msg.Topic, topicRespond):
// 		go msgListen(msg.Topic, msg.Payload)
// 	case umq.Match(msg.Topic, topicReport):
// 		go armReportGz(msg.Topic, msg.Payload)
// 	}
// }
func (m *MQC) SetMessageCallback(callback func(msg *Message)) {
	m.Service.MessageCallback = func(msg *Message) error {
		callback(msg)
		return nil
	}
}

/*
New 创建操作MQ消息的对象
config: 构建mq需要的配置
*/
func New(config Config) *MQC {
	cfg := mqc.NewConfig(config.BrokerUrl)
	if config.KeepAlive != 0 {
		cfg.KeepAlive = (time.Second * config.KeepAlive).String()
	}
	if cfg.ClientID != "" {
		cfg.ClientID = config.ClientID
	}
	cfg.CleanSession = true
	if config.DisableCleanSession {
		cfg.CleanSession = false
	}
	s := mqc.NewService()
	s.ResubscribeAllSubscriptions = true
	if config.MaxReconnectDelay != 0 {
		s.MaxReconnectDelay = time.Second * config.MaxReconnectDelay
	}

	m := MQC{
		cfg:            cfg,
		Service:        s,
		PublishTimeout: time.Second,
	}
	if config.PublishTimeout != 0 {
		m.PublishTimeout = time.Second * config.PublishTimeout
	}
	return &m
}
