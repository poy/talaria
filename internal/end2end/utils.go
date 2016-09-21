package end2end

import "net"

func AvailablePort() int {
	add, _ := net.ResolveTCPAddr("tcp", ":0")
	l, _ := net.ListenTCP("tcp", add)
	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port
	return port
}
