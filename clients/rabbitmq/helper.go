package rabbitmq

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"
)

// GetByEnvOf 通过env 获取 string 值
func GetByEnvOf(key string, def ...string) string {
	def = append(def, "")
	var v = os.Getenv(key)
	if v != "" {
		return v
	}
	return def[0]
}

// GetBoolByEnvOf 通过env 获取 bool 值
func GetBoolByEnvOf(key string, def ...bool) bool {
	def = append(def, false)
	var v = os.Getenv(key)
	if v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		switch v {
		case "yes":
			return true
		case "Yes":
			return true
		case "YES":
			return true
		case "ON":
			return true
		case "on":
			return true
		case "On":
			return true
		case "1":
			return true
		}
	}
	return def[0]
}

// GetDurationByEnvOf 通过env 获取 duration 值
func GetDurationByEnvOf(key string, def ...time.Duration) time.Duration {
	def = append(def, 0)
	var v = os.Getenv(key)
	if v != "" {
		if b, err := time.ParseDuration(v); err == nil {
			return b
		}
		if n, err := strconv.Atoi(v); err == nil {
			return time.Duration(n)
		}
	}
	return 0
}

// GetJsonByEnvBind 通过env 获取 object 值
func GetJsonByEnvBind(key string, v interface{}) error {
	if v == nil {
		_, file, line, _ := runtime.Caller(0)
		return fmt.Errorf("%s, at line : %d ,%s", file, line, "GetJsonByEnvBind.Error v is Nil")
	}
	var data = os.Getenv(key)
	if data == "" {
		return fmt.Errorf("Env.Nil")
	}
	if err := json.Unmarshal([]byte(data), v); err != nil {
		return err
	}
	return nil
}
