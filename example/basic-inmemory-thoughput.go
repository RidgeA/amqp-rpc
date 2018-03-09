package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/RidgeA/amqp-rpc"
	"github.com/RidgeA/amqp-rpc/transport"
)

func main() {

	name := "test"
	t := transport.NewINMemory()
	t.Initialize()

	log.SetFlags(log.Lshortfile | log.LstdFlags)

	client := rpc.NewClient(name, rpc.SetTransport(t))

	server := rpc.NewServer(name, rpc.SetTransport(t))

	server.RegisterHandler("write", func(payload []byte) ([]byte, error) {
		time.Sleep(500 * time.Millisecond)
		fmt.Printf("%s: server log: %s\n", time.Now().Format("15:04:05.999999"), string(payload))
		return nil, nil
	}, rpc.SetHandlerThroughput(2))

	if err := client.Start(); err != nil {
		log.Fatal(err.Error())
	}
	defer client.Shutdown()

	if err := server.Start(); err != nil {
		log.Fatal(err.Error())
	}
	defer server.Shutdown()

	for i := 0; i < 10; i++ {
		_, err := client.Call(context.Background(), "write", []byte(strconv.Itoa(i)+":hello!"), false)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	time.Sleep(3000 * time.Millisecond)
}
