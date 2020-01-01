package umq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/michelia/ulog"
)

type (
	MessageHandler = mqtt.MessageHandler
	Client         = mqtt.Client
	Message        = mqtt.Message
)

var ErrConnectTimeout = errors.New("mqc Connect token.WaitTimeout")
var ErrPublishTimeout = errors.New("mqc Publish token.WaitTimeout")

// Config 构建mq需要的配置
type Config struct {
	BrokerAddr           string
	UserName             string
	Password             string
	KeepAlive            int
	DisableCleanSession  bool
	ConnectTimeout       int // 单位ms
	PublishTimeout       int // 单位ms
	MaxReconnectInterval int
}

type FuncClientID func() string

type subscribe struct {
	topic    string
	qos      byte
	callback MessageHandler
}

type MQ struct {
	slog         ulog.Logger
	mqc          mqtt.Client
	mutex        sync.Mutex
	Config       Config
	funcClientID FuncClientID
	subs         []subscribe
}

/*
New 创建操作MQ消息的对象

slog: 带session的log
config: 构建mq需要的配置
*/
func New(slog ulog.Logger, config Config) *MQ {
	if config.ConnectTimeout == 0 {
		slog.Panic().Msg("ConnectTimeout except value > 0 ms")
	}
	if config.PublishTimeout == 0 {
		slog.Panic().Msg("PublishTimeout except value > 0 ms")
	}
	mq := MQ{
		slog:   slog,
		Config: config,
	}
	return &mq
}

func (mq *MQ) SetClientID(funcClientID FuncClientID) {
	mq.funcClientID = funcClientID
}

func (mq *MQ) AddSubscrAdd(topic string, qos int, callback MessageHandler) {
	mq.subs = append(mq.subs, subscribe{
		topic:    topic,
		qos:      byte(qos),
		callback: callback,
	})
}

func (mq *MQ) newClient() mqtt.Client {
	clientID := mq.funcClientID()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(mq.Config.BrokerAddr)
	opts.SetClientID(clientID) // 每次连接的时候都更换ClientID
	opts.SetUsername(mq.Config.UserName)
	opts.SetPassword(mq.Config.Password)
	opts.SetProtocolVersion(3)
	if mq.Config.KeepAlive != 0 {
		opts.SetKeepAlive(time.Second * time.Duration(mq.Config.KeepAlive))
	}
	if mq.Config.DisableCleanSession == true {
		opts.SetCleanSession(false) // 默认是 CleanSession 即不建队列, 不持久化信息
	}
	// opts.SetConnectRetry(true)
	opts.SetConnectTimeout(time.Millisecond * time.Duration(mq.Config.ConnectTimeout))
	// opts.SetCleanSession(false) // 默认是清除session, 重连后Subscribe都失效了, 所以要设置为false, 自带的重连不需要设置这个参数
	fmt.Println("ClientID: " + opts.ClientID)
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		mq.slog.Warn().Str("clientID", clientID).Err(err).Msg("error disconnect EMQ")
	})
	opts.SetOnConnectHandler(func(cli mqtt.Client) {
		// 添加订阅
		for _, v := range mq.subs {
			cli.Subscribe(v.topic, v.qos, v.callback)
		}
		mq.slog.Warn().Str("clientID", clientID).Msg("success connect EMQ and subscribe")
	})
	return mqtt.NewClient(opts)
}

// Connect 更换client 再次连接
func (mq *MQ) Connect() error {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()
	if mq.mqc != nil && mq.mqc.IsConnected() {
		mq.mqc.Disconnect(100)
	}
	mq.mqc = mq.newClient()
	token := mq.mqc.Connect()
	if !token.WaitTimeout(time.Millisecond * time.Duration(mq.Config.ConnectTimeout)) {
		return ErrConnectTimeout
	}
	return token.Error()
}

// Publish 尝试发送三次
func (mq *MQ) Publish(slog ulog.Logger, topic string, qos uint, retained bool, payload interface{}) error {
	var err error
	for i := 0; i < 3; i++ {
		token := mq.mqc.Publish(topic, byte(qos), retained, payload)
		if !token.WaitTimeout(time.Millisecond * time.Duration(mq.Config.PublishTimeout)) {
			slog.Warn().Caller().Err(ErrPublishTimeout).Int("retry", i).Msg("重连后再Publish")
			err = mq.Connect()
			if err != nil {
				slog.Error().Caller().Err(err).Int("retry", i).Msg("再次重连后再Publish")
			}
			continue
		}
		switch err = token.Error(); err {
		case nil:
			slog.Info().Int("retry", i).Msg("已下发")
			return nil
		case mqtt.ErrNotConnected:
			err = mq.Connect()
			if err != nil {
				slog.Error().Caller().Err(err).Int("retry", i).Msg("没有连接,重连后再Publish")
			}
			continue
		default:
			slog.Error().Caller().Err(err).Int("retry", i).Msg("mq.mqc.Publish other error")
			continue
			// return err
		}
	}
	return err
}
