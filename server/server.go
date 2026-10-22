package server

import (
	"antardas/tcp-tunnel/protocol"
	"fmt"
	"io"
	"net"
)

func Start() {
	listener, err := net.Listen("tcp", ":8080")

	if err != nil {
		panic(err)
	}

	defer listener.Close()

	fmt.Println("Server is listening 8080")

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Accept error", err)
			continue
		}

		go handleConnection(conn)

	}

}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {

		response, err := protocol.ReadMessage(conn)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
			} else {
				fmt.Println("Read Error: ", err)
			}

			return
		}

		fmt.Println("Client Sent: ", string(response))

		if err := protocol.WriteMessage(conn, response); err != nil {
			fmt.Println("Write Error: ", err)
			return
		}
	}

}
