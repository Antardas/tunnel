package main

import (
	"antardas/tcp-tunnel/client"
	"antardas/tcp-tunnel/server"
	"flag"
)

func main() {
	mode := flag.String("mode", "", "server or client")
	flag.Parse()

	switch *mode {
	case "server":
		server.Start()
	case "client":
		client.Connect()
	default:
		println("Use -mode=server or -mode=client")
	}
}
