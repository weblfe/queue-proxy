package config

// ServerConfig 服务配置
type ServerConfig struct {
	TcpServCfg   TCPServerConfig               // 代理服务tcp server 配置
	ApiServCfg   *HttpServerConfig             // API 服务配置
	GrpcServCfg  *GrpcServerConfig             // grpc 服务配置
	WebSocket    *WebSocketConfig              // webSocket 服务配置
	Auths        map[string][]User             // 认证用户-配置
	QueueDrivers map[string]QueueDriverEntry   // 队列驱动
	Storages     map[string]StorageDriverEntry // 存储驱动
	Logger       map[string]LoggerEntry        // 日志
	AppType      string                        // 应用服务类型
}

// LoggerEntry 日志配置
type LoggerEntry struct {
	Driver        StorageDriver
	Level         string   // 日志level
	FilterPattern []string // 过滤正则
	Format        string   // 格式化
}

// TCPServerConfig tcp 服务配置
type TCPServerConfig struct {
	Addr string
}

// HttpServerConfig http 接口服务
type HttpServerConfig struct {
	Addr string
}

// GrpcServerConfig grpc 服务
type GrpcServerConfig struct {
	Addr string
}

// WebSocketConfig webSocket 服务
type WebSocketConfig struct {
	Addr    string
	Options map[string]interface{}
}

// User 用户
type User struct {
	User     string
	Password string
	Role     string
}

// QueueDriverEntry 队列配置
type QueueDriverEntry struct {
	Driver  QueueDriver
	Addr    string
	Options map[string]interface{}
}

// StorageDriverEntry 存储配置
type StorageDriverEntry struct {
	Driver  StorageDriver
	ConnUrl string
}
