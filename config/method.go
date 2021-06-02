package config

func NewServerCfg() *ServerConfig {
	var _cfg = &ServerConfig{}
	return _cfg.init()
}

func (cfg *ServerConfig) init() *ServerConfig {
	if cfg.Logger == nil {
		cfg.Logger = make(map[string]LoggerEntry, 4)
	}
	if cfg.QueueDrivers == nil {
		cfg.QueueDrivers = make(map[string]QueueDriverEntry, 4)
	}
	if cfg.Storages == nil {
		cfg.Storages = make(map[string]StorageDriverEntry, 4)
	}
	if cfg.Auths == nil {
		cfg.Auths = make(map[string][]User, 1)
	}
	return cfg
}

func (cfg *ServerConfig) Reload(handler func(cfg *ServerConfig) error) error {
	if handler != nil {
		return handler(cfg)
	}
	return nil
}
