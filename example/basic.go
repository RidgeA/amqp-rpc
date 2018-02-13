package main

import (
	"gitlab.com/RidgeA/amqp-rpc"
	"log"
	"strings"
	"fmt"
	"context"
)

var url = "amqp://guest:guest@127.0.0.2:5672"
var name = "test"

func upper(payload []byte) ([]byte, error) {
	return []byte(strings.ToUpper(string(payload))), nil
}

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	client := rpc.NewRPCClient(name, url)

	server := rpc.NewRPCServer(name, url)

	server.RegisterHandler("upper", upper)

	if err := client.Start(); err != nil {
		log.Fatal(err.Error())
	}
	defer client.Shutdown()

	if err := server.Start(); err != nil {
		log.Fatal(err.Error())
	}
	defer server.Shutdown()


	response, err := client.Call(context.Background(), "upper", []byte("hello!"), true)
	fmt.Printf("Response: %s\n", string(response))
	if err != nil {
		log.Fatal(err.Error())
	}

	response, err = client.Call(context.Background(), "upper", []byte("bye!"), true)
	fmt.Printf("Response: %s\n", string(response))
	if err != nil {
		log.Fatal(err.Error())
	}
}
