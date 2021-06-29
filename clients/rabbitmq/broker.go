package rabbitmq

import (
	"crypto/tls"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"net"
	"sync"
	"time"
)

type Broker struct {
	server     string
	ssl        bool
	vhost      string
	userPass   string
	ca         string
	cert       string
	key        string
	quePrefix  string
	topics     []Topic
	tlsConfig  *tls.Config
	proxyAddr  string
	heartbeat  time.Duration
	locker     sync.RWMutex
	connectors map[string]*amqp.Connection
}

type BrokerCfg struct {
	Server    string        `json:"server"`
	Ssl       bool          `json:"ssl"`
	Vhost     string        `json:"vhost"`
	UserPass  string        `json:"user_pass,omitempty"`
	Ca        string        `json:"ca,omitempty"`
	Cert      string        `json:"cert,omitempty"`
	Key       string        `json:"key,omitempty"`
	QuePrefix string        `json:"queue_prefix,omitempty"`
	ProxyAddr string        `json:"proxy_addr,omitempty"`
	Heartbeat time.Duration `json:"heartbeat,default=10s"`
	Topics    []TopicInfo   `json:"topics,omitempty"`
}

const (
	MqPassEnvKey            = "RABBITMQ_USER_PASS"
	MqLocaleEnvKey          = "RABBITMQ_PROXY_LOCALE"
	defaultMqPass           = "guest"
	defaultServer           = "127.0.0.1"
	defaultVhost            = "/default"
	defaultLocale           = "en_US"
	defaultHearBeatDuration = 10 * time.Second
	defaultDailNetworkTcp4  = "tcp4"
)

func CreateBroker(info *BrokerCfg) *Broker {
	if info == nil {
		return nil
	}
	return info.createBroker()
}

func (info *BrokerCfg) createBroker() *Broker {
	var broker = NewBroker()
	broker.server = info.Server
	broker.key = info.Key
	broker.ca = info.Ca
	broker.cert = info.Cert
	broker.ssl = info.Ssl
	broker.userPass = info.UserPass
	broker.proxyAddr = info.ProxyAddr
	broker.quePrefix = info.QuePrefix
	broker.heartbeat = info.Heartbeat
	for _, v := range info.Topics {
		broker.AddTopic(v.Create())
	}
	return broker
}

func (b *Broker) GetConnUrl() string {
	return fmt.Sprintf("%s://%s@%s/%s", b.GetProtocol(), b.GetUserPass(), b.GetServer(), b.GetVhost())
}

func (b *Broker) GetServer() string {
	b.locker.RLocker().Lock()
	defer b.locker.RUnlock()
	if len(b.server) <= 0 {
		return defaultServer
	}
	return b.server
}

func NewBroker() *Broker {
	return &Broker{
		locker:     sync.RWMutex{},
		connectors: make(map[string]*amqp.Connection),
	}
}

func (b *Broker) GetVhost() string {
	b.locker.RLocker().Lock()
	defer b.locker.RUnlock()
	if len(b.vhost) <= 0 {
		return defaultVhost
	}
	return b.vhost
}

func (b *Broker) GetUserPass() string {
	b.locker.RLocker().Lock()
	defer b.locker.RUnlock()
	if b.userPass == "" {
		b.userPass = GetByEnvOf(MqPassEnvKey, defaultMqPass)
	}
	return b.userPass
}

func (b *Broker) GetProtocol() string {
	b.locker.RLocker().Lock()
	defer b.locker.RUnlock()
	if b.ssl {
		return protocolAmqpSSL
	}
	return protocolAmqp
}

func (b *Broker) GetConnector() (*amqp.Connection, error) {
	if len(b.proxyAddr) > 0 {
		return amqp.DialConfig(b.GetConnUrl(), b.getAmqpConfig())
	}
	return amqp.DialTLS(b.GetConnUrl(), b.GetTlsConfig())
}

func (b *Broker) GetConnection() *amqp.Connection {
	var id = b.getHash()
	b.locker.Lock()
	defer b.locker.Unlock()
	if conn, ok := b.connectors[id]; ok {
		return conn
	}
	conn, err := b.GetConnector()
	if err != nil {
		log.Println("Broker.GetConnection.Error:", err.Error())
		return nil
	}
	b.connectors[id] = conn
	return conn
}

func (b *Broker) getHash() string {
	return fmt.Sprintf("%s.%s.%v", b.GetConnUrl(), b.proxyAddr, b.ssl)
}

func (b *Broker) getAmqpConfig() amqp.Config {
	return amqp.Config{
		Heartbeat:       b.getHearBeatTime(),
		TLSClientConfig: b.GetTlsConfig(),
		Locale:          GetByEnvOf(MqLocaleEnvKey, defaultLocale),
		Dial:            b.getDailProcessor(b.proxyAddr),
	}
}

func (b *Broker) getDailProcessor(proxyAddr string) func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		return net.Dial(defaultDailNetworkTcp4, proxyAddr)
	}
}

func (b *Broker) getHearBeatTime() time.Duration {
	b.locker.RLocker().Lock()
	defer b.locker.RUnlock()
	if b.heartbeat <= 0 {
		return defaultHearBeatDuration
	}
	return b.heartbeat
}

func (b *Broker) SetProxyAddr(add string) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	b.proxyAddr = add
	return b
}

func (b *Broker) SetVhost(vhost string) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	b.vhost = vhost
	return b
}

func (b *Broker) SetUserPass(userPass string) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	b.userPass = userPass
	return b
}

func (b *Broker) SetQueuePrefix(prefix string) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	b.quePrefix = prefix
	return b
}

func (b *Broker) GetTlsConfig() *tls.Config {
	b.locker.Lock()
	defer b.locker.Unlock()
	if b.tlsConfig == nil {
		return nil
	}
	return b.tlsConfig
}

func (b *Broker) GetTlsState(on bool) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	b.ssl = on
	return b
}

func (b *Broker) SetTlsConfig(cnf *tls.Config) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	if cnf != nil {
		b.tlsConfig = cnf
	}
	return b
}

func (b *Broker) SetKey(keyFile string) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	b.key = keyFile
	return b
}

func (b *Broker) SetCa(caFile string) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	b.ca = caFile
	return b
}

func (b *Broker) SetCert(certFile string) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	b.cert = certFile
	return b
}

func (b *Broker) AddTopic(topic Topic) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	for _, v := range b.topics {
		if v.Equal(topic) {
			return b
		}
	}
	b.topics = append(b.topics, topic)
	return b
}

func (b *Broker) GetTopics() []Topic {
	b.locker.RLocker().Lock()
	defer b.locker.RUnlock()
	return b.topics
}

func (b *Broker) RemoveTopic(topic Topic) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	for i, v := range b.topics {
		if v.Equal(topic) {
			b.topics = append(b.topics[:i-1], b.topics[i:]...)
			return b
		}
	}
	return b
}

func (b *Broker) Close() error {
	b.locker.Lock()
	defer b.locker.Unlock()
	for key, v := range b.connectors {
		if !v.IsClosed() {
			if err := v.Close(); err != nil {
				return err
			}
		}
		delete(b.connectors, key)
	}
	b.topics = []Topic{}
	return nil
}
