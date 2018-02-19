package main

import (
	"gitlab.com/RidgeA/amqp-rpc"
	"log"
	"strings"
	"fmt"
	"context"
	"time"
	"gitlab.com/RidgeA/amqp-rpc/transport"
)

func main() {

	name := "test"
	t := transport.NewINMemory()
	t.Initialize()

	log.SetFlags(log.Lshortfile | log.LstdFlags)

	client := rpc.NewClient(name, rpc.SetTransport(t))

	server := rpc.NewServer(name, rpc.SetTransport(t))

	server.RegisterHandler("upper", func(payload []byte) ([]byte, error) {
		return []byte(strings.ToUpper(string(payload))), nil
	})

	server.RegisterHandler("lower", func(payload []byte) ([]byte, error) {
		return []byte(strings.ToLower(string(payload))), nil
	})

	server.RegisterHandler("write", func(payload []byte) ([]byte, error) {
		fmt.Printf("Server log: %s\n", string(payload))
		return nil, nil
	})

	if err := client.Start(); err != nil {
		log.Fatal(err.Error())
	}
	defer client.Shutdown()

	if err := server.Start(); err != nil {
		log.Fatal(err.Error())
	}
	defer server.Shutdown()

	response, err := client.Call(context.Background(), "upper", []byte("hello!"), true)
	if err != nil {
		log.Fatal(err.Error())
	}
	_, err = client.Call(context.Background(), "write", response, false)
	if err != nil {
		log.Fatal(err.Error())
	}

	response, err = client.Call(context.Background(), "lower", []byte("BYE!"), true)
	_, err = client.Call(context.Background(), "write", response, false)
	if err != nil {
		log.Fatal(err.Error())
	}

	time.Sleep(100 * time.Millisecond)
}
