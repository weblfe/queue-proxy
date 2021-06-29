package rabbitmq

import "os"

func GetByEnvOf(key string,def...string) string {
	def = append(def, "")
	var v= os.Getenv(key)
	if v != "" {
		return v
	}
	return def[0]
}
