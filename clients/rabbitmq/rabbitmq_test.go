package rabbitmq

import (
	"os"
	"testing"
)

func Test_NewConnection(t *testing.T) {
	_ = os.Setenv("RABBITMQ_DEFAULT_AUTH", "dev:dev")
	var broker = CreateBrokerByEnv("default")
	conn := broker.GetConnection()
	if conn == nil {
		t.Error("conn error")
		return
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Error(err.Error())
	}
	if ch != nil {
		_ = ch.Close()
	}
}
