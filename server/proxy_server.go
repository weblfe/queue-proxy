package server

import (
	"net"
	"sync"
)

type ProxyServer struct {
	sync.Mutex
	connsPool map[string]*net.TCPConn
	entryPool map[string]interface{}
}
