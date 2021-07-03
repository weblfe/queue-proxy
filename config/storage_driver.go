package config

// StorageDriver 存储驱动
type StorageDriver string

const (
	DriverNil        StorageDriver = ""
	DriverFile       StorageDriver = "file"
	DriverMysql      StorageDriver = "mysql"
	DriverLocal      StorageDriver = "local"
	DriverRedis      StorageDriver = "redis"
	DriverMongo      StorageDriver = "mongo"
	DriverClickHouse StorageDriver = "clickhouse"
	DriverInfluxDb   StorageDriver = "influxdb"
)
