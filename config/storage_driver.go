package config

// 存储驱动
type StorageDriver string

const (
	DriverNil        StorageDriver = ""
	DriverFile       StorageDriver = "file"
	DriverMysql      StorageDriver = "mysql"
	DriverRedis      StorageDriver = "redis"
	DriverMongo      StorageDriver = "mongo"
	DriverClickHouse StorageDriver = "clickhouse"
	DriverInfluxDb   StorageDriver = "influxdb"
)
