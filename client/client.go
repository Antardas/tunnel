package client

import (
	"antardas/tcp-tunnel/protocol"
	"bytes"
	"fmt"
	"net"
)

func Connect() {
	fmt.Println("Connecting to the public server")

	conn, err := net.Dial("tcp", "localhost:8080")

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	fmt.Println("Conneted to the server")
	for i := 0; i < 3; i++ {

		// _, err = conn.Write(fmt.Appendf(nil, "Hello from Go client! i = %d\n", i))
		data := bytes.Repeat([]byte("A"), (1024 * 1024 * 10))

		if err := protocol.WriteMessage(conn, data); err != nil {
			panic(err)
		}

		response, err := protocol.ReadMessage(conn)
		if err != nil {
			panic(err)
		}
		fmt.Println("Sent:", len(data), "Received:", len(response))
	}

}
