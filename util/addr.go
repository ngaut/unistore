package util

import "strings"

func GRPCAddr(addr string) string {
	idx := strings.IndexByte(addr, ',')
	return addr[:idx]
}

func TurboAddr(addr string) string {
	idx := strings.IndexByte(addr, ',')
	return addr[idx+1:]
}
