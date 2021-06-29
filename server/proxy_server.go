package server

import (
	"net"
	"sync"
)

type ProxyServer struct {
	locker sync.Mutex
	connsPool map[string]*net.TCPConn
	entryPool map[string]interface{}
}
