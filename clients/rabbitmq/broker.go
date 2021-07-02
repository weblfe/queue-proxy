package rabbitmq

import (
	"crypto/md5"
	"crypto/tls"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	MqAuthEnvKey            = "RABBITMQ_AUTH"
	MqLocaleEnvKey          = "RABBITMQ_PROXY_LOCALE"
	defaultServer           = "127.0.0.1"
	defaultVhost            = "default"
	defaultAuth             = "guest:guest"
	defaultQueuePrefix      = "queue_"
	defaultLocale           = "en_US"
	defaultHearBeatDuration = 10 * time.Second
	defaultDailNetworkTcp4  = "tcp4"
	mqEnvKeyPrefix          = "RABBITMQ"
)

type (

	// Broker 代理链接器工具类
	Broker struct {
		server     string
		ssl        bool
		vhost      string
		userPass   string
		certFile   string
		keyFile    string
		quePrefix  string
		tlsConfig  *tls.Config
		proxyAddr  string
		heartbeat  time.Duration
		locker     sync.RWMutex
		connectors map[string]*amqp.Connection
	}

	// 代理链接配置
	BrokerCfg struct {
		Server    string        `json:"broker"`
		Ssl       bool          `json:"ssl"`
		Vhost     string        `json:"vhost"`
		UserPass  string        `json:"user_pass,omitempty"`
		Cert      string        `json:"cert_file,omitempty"`
		Key       string        `json:"key_file,omitempty"`
		QuePrefix string        `json:"queue_prefix,omitempty"`
		ProxyAddr string        `json:"proxy_addr,omitempty"`
		Heartbeat time.Duration `json:"heartbeat,default=10s"`
	}
)

// 获取
func CreateBroker(info *BrokerCfg) *Broker {
	if info == nil {
		return nil
	}
	return info.createBroker()
}

// 解析url
func ParseUrlBrokerCfg(dns string) (*BrokerCfg, error) {
	var cfg = &BrokerCfg{}
	info, err := url.Parse(dns)
	if err != nil {
		return nil, err
	}
	if info.User != nil {
		user := info.User.Username()
		password, _ := info.User.Password()
		cfg.UserPass = user + ":" + password
	}
	if strings.Contains(info.Scheme, "s") {
		cfg.Ssl = true
	}
	cfg.Vhost = info.Path
	cfg.Server = info.Host
	if info.RawQuery != "" {
		values, err := url.ParseQuery(info.RawQuery)
		if err != nil {
			return cfg, nil
		}
		// 通过参数设置
		for k, v := range values {
			num := len(v)
			if num <= 0 {
				continue
			}
			if num == 1 {
				cfg.Set(k, v[0])
			} else {
				cfg.Set(k, v)
			}
		}
	}
	return cfg, nil
}

// 设置值
func (cfg *BrokerCfg) Set(key string, v interface{}) *BrokerCfg {
	switch v.(type) {
	case string:
		var str = v.(string)
		switch key {
		case "proxyAddr":
			cfg.ProxyAddr = str
		case "proxy_addr":
			cfg.ProxyAddr = str
		case "Vhost":
			cfg.Vhost = str
		case "vhost":
			cfg.Vhost = str
		case "UserPass":
			cfg.UserPass = str
		case "user_pass":
			cfg.UserPass = str
		case "certFile":
			cfg.Cert = str
		case "cert_file":
			cfg.Cert = str
		case "key_file":
			cfg.Key = str
		case "keyFile":
			cfg.Key = str
		case "QueuePrefix":
			cfg.QuePrefix = str
		case "queue_prefix":
			cfg.QuePrefix = str
		case "Heartbeat":
			if d, err := time.ParseDuration(str); err == nil {
				cfg.Heartbeat = d
			}
		case "heartbeat":
			if d, err := time.ParseDuration(str); err == nil {
				cfg.Heartbeat = d
			}
		}

	case bool:
		var b = v.(bool)
		switch key {
		case "ssl":
			cfg.Ssl = b
		case "ssl_on":
			cfg.Ssl = b
		}

	case int64:
		var d = v.(int64)
		switch key {
		case "Heartbeat":
			cfg.Heartbeat = time.Duration(d)
		case "heartbeat":
			cfg.Heartbeat = time.Duration(d)
		}
	}

	return cfg
}

func (cfg *BrokerCfg) createBroker() *Broker {
	var broker = NewBroker()
	return broker.SetByBrokerCfg(*cfg)
}

// 同env 创建
func CreateBrokerByEnv(namespace ...string) *Broker {
	var info = GetBrokerInfoByEnv(namespace...)
	return info.createBroker()
}

// 通过环境变成创建配置
func GetBrokerInfoByEnv(namespace ...string) BrokerCfg {
	namespace = append(namespace, "")
	var (
		info   = BrokerCfg{
			Server:    GetByEnvOf(getKeyByNamespace("SERVER", namespace[0]), defaultServer),
			UserPass:  GetByEnvOf(getKeyByNamespace("AUTH", namespace[0]), defaultAuth),
			Vhost:     GetByEnvOf(getKeyByNamespace("VHOST", namespace[0]), defaultVhost),
			Ssl:       GetBoolByEnvOf(getKeyByNamespace("SSL_ON", namespace[0]), false),
			Cert:      GetByEnvOf(getKeyByNamespace("SSL_CERT_FILE", namespace[0]), ""),
			Key:       GetByEnvOf(getKeyByNamespace("SSL_KEY_FILE", namespace[0]), ""),
			QuePrefix: GetByEnvOf(getKeyByNamespace("QUEUE_PREFIX", namespace[0]), defaultQueuePrefix),
			ProxyAddr: GetByEnvOf(getKeyByNamespace("PROXYADDR", namespace[0]), ""),
			Heartbeat: GetDurationByEnvOf(getKeyByNamespace("HEARTBEAT_DURATION", namespace[0]), heartbeat),
		}
	)
	return info
}

// 通过命名前缀构建key
func getKeyByNamespace(key string, namespace string) string {
	if namespace == "" {
		return strings.ToUpper(mqEnvKeyPrefix + "_" + key)
	}
	return strings.ToUpper(mqEnvKeyPrefix + "_" + namespace + "_" + key)
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
		b.userPass = GetByEnvOf(MqAuthEnvKey, defaultAuth)
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

func (b *Broker) GetQueuePrefix() string {
	b.locker.RLocker().Lock()
	defer b.locker.RUnlock()
	return b.quePrefix
}

func (b *Broker) GetConnector() (*amqp.Connection, error) {
	if len(b.proxyAddr) > 0 {
		return amqp.DialConfig(b.GetConnUrl(), b.getAmqpConfig())
	}
	return amqp.DialTLS(b.GetConnUrl(), b.GetTlsConfig())
}

func (b *Broker) GetConnection() *amqp.Connection {
	var id = b.getConnectorId()

	conn, ok := b.connectors[id]
	if ok && !conn.IsClosed() {
		return conn
	}
	conn, err := b.GetConnector()
	if err != nil {
		log.Panicln("[RABBITMQ_CLIENT_Broker] Broker.GetConnection.Error:", err.Error())
		return nil
	}
	b.locker.Lock()
	defer b.locker.Unlock()
	b.connectors[id] = conn
	return conn
}

func (b *Broker) getConnectorId() string {
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
