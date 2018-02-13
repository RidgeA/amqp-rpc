package main

import (
	"gitlab.com/RidgeA/amqp-rpc"
	"log"
	"strings"
	"fmt"
	"context"
	"time"
)

func main() {
	var url = "amqp://guest:guest@127.0.0.1:5672"
	var name = "test"

	log.SetFlags(log.Lshortfile | log.LstdFlags)

	duplex := rpc.NewDuplex(name, rpc.SetUrl(url))

	duplex.RegisterHandler("upper", func(payload []byte) ([]byte, error) {
		return []byte(strings.ToUpper(string(payload))), nil
	})

	duplex.RegisterHandler("lower", func(payload []byte) ([]byte, error) {
		return []byte(strings.ToLower(string(payload))), nil
	})

	duplex.RegisterHandler("write", func(payload []byte) ([]byte, error) {
		fmt.Printf("Server log: %s\n", string(payload))
		return nil, nil
	})

	if err := duplex.Start(); err != nil {
		log.Fatal(err.Error())
	}
	defer duplex.Shutdown()

	response, err := duplex.Call(context.Background(), "upper", []byte("hello!"), true)
	if err != nil {
		log.Fatal(err.Error())
	}

	_, err = duplex.Call(context.Background(), "write", response, false)
	if err != nil {
		log.Fatal(err.Error())
	}

	response, err = duplex.Call(context.Background(), "lower", []byte("BYE!"), true)
	_, err = duplex.Call(context.Background(), "write", response, false)
	if err != nil {
		log.Fatal(err.Error())
	}

	time.Sleep(100 * time.Millisecond)
}
