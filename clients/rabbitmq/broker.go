package rabbitmq

import (
	"crypto/md5"
	"crypto/tls"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"net"
	"sync"
	"time"
)

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

type Broker struct {
	server     string
	ssl        bool
	vhost      string
	userPass   string
	certFile   string
	keyFile    string
	quePrefix  string
	topics     []Topic
	tlsConfig  *tls.Config
	proxyAddr  string
	heartbeat  time.Duration
	locker     sync.RWMutex
	connectors map[string]*amqp.Connection
}

type BrokerCfg struct {
	Server    string        `json:"broker"`
	Ssl       bool          `json:"ssl"`
	Vhost     string        `json:"vhost"`
	UserPass  string        `json:"user_pass,omitempty"`
	Cert      string        `json:"cert_file,omitempty"`
	Key       string        `json:"key_file,omitempty"`
	QuePrefix string        `json:"queue_prefix,omitempty"`
	ProxyAddr string        `json:"proxy_addr,omitempty"`
	Heartbeat time.Duration `json:"heartbeat,default=10s"`
	Topics    []TopicInfo   `json:"topics,omitempty"`
}

func CreateBroker(info *BrokerCfg) *Broker {
	if info == nil {
		return nil
	}
	return info.createBroker()
}

func (info *BrokerCfg) createBroker() *Broker {
	var broker = NewBroker()
	return broker.SetByBrokerCfg(*info)
}

func (b *Broker) SetByBrokerCfg(info BrokerCfg) *Broker {
	b.server = info.Server
	b.keyFile = info.Key
	b.certFile = info.Cert
	b.ssl = info.Ssl
	b.userPass = info.UserPass
	b.proxyAddr = info.ProxyAddr
	b.quePrefix = info.QuePrefix
	b.heartbeat = info.Heartbeat
	for _, v := range info.Topics {
		b.AddTopic(v.Create())
	}
	return b
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

func (b *Broker) SetServer(server string) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	b.server = server
	return b
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
		log.Panicln("Broker.GetConnection.Error:", err.Error())
		return nil
	}
	b.connectors[id] = conn
	return conn
}

func (b *Broker) getHash() string {
	return fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%s.%s.%v", b.GetConnUrl(), b.proxyAddr, b.ssl))))
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
		if b.keyFile != "" && b.certFile != "" {
			b.tlsConfig = b.createTlsConfig(b.keyFile, b.certFile)
		}
		if b.tlsConfig == nil {
			return nil
		}
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
	b.keyFile = keyFile
	return b
}

func (b *Broker) SetCert(certFile string) *Broker {
	b.locker.Lock()
	defer b.locker.Unlock()
	b.certFile = certFile
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

func (b *Broker) createTlsConfig(keyFile, certFile string) *tls.Config {
	var cert, err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("Broker.createTlsConfig.Error: %s", err.Error())
		return nil
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
}
