package main

import (
	"antardas/tcp-tunnel/client"
	"antardas/tcp-tunnel/server"
	"flag"
	"fmt"
	"os"
)

func main() {
	mode := flag.String("mode", "", "server or client")
	clientId := flag.String("id", "", "Tunnel Client ID")
	flag.Parse()

	switch *mode {
	case "server":
		server.Start()
	case "client":
		if *clientId == "" {
			fmt.Fprintln(os.Stderr, "Error: client ID is required")
			os.Exit(1)
		}
		client.Connect(*clientId)
	default:
		println("Use -mode=server or -mode=client")
	}
}
