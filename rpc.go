package rpc

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"strconv"
	"context"
)

var (
	silentLog = func(format string, args ...interface{}) {}
	errorLog  = log.Printf
)

type (
	Runable interface {
		Start() error
	}

	Terminable interface {
		Shutdown()
	}
	Caller interface {
		Call(context.Context, string, []byte, bool) ([]byte, error)
	}

	Registerer interface {
		RegisterHandler(string, HandlerFunc)
	}

	Server interface {
		Runable
		Terminable
		Registerer
	}

	Client interface {
		Runable
		Terminable
		Caller
	}

	LogFunc func(string, ...interface{})

	HandlerFunc func([]byte) ([]byte, error)

	handler struct {
		name    string
		channel *amqp.Channel
		handler HandlerFunc
	}

	AMQPHandler func(amqp.Delivery) error

	Logger struct {
		error, info, debug LogFunc
	}

	RPC struct {
		Logger

		tag          string
		url          string
		name         string
		exchangeName string

		extConn bool
		conn    *amqp.Connection

		outChannel   *amqp.Channel
		outQueueName string
	}

	ServerImpl struct {
		*RPC

		handlers map[string]*handler
	}

	ClientImpl struct {
		*RPC

		inChannel   *amqp.Channel
		inQueueName string

		listeners map[string]chan amqp.Delivery
	}
)

func NewRPCServer(name, url string) Server {
	r := newRPC(name, url)
	r.debug("Creating server for %s", r.name)
	s := new(ServerImpl)
	s.RPC = r
	s.handlers = make(map[string]*handler)
	s.tag = createTag(s.name, true)
	s.exchangeName = exchangeName(s.name)
	return s
}

func NewRPCClient(name, url string) Client {
	r := newRPC(name, url)
	r.debug("Creating client for %s", r.name)
	c := new(ClientImpl)
	c.RPC = r
	c.listeners = make(map[string]chan amqp.Delivery)
	c.tag = createTag(r.name, false)
	c.exchangeName = exchangeName(c.name)
	c.outQueueName = outQueueName(c.tag)
	c.inQueueName = inQueueName(c.tag)
	return c
}

func newRPC(name, url string) (rpc *RPC) {
	rpc = new(RPC)
	rpc.url = url
	rpc.name = name
	rpc.error = errorLog
	rpc.info = silentLog
	rpc.debug = silentLog
	return
}

func (rpc *RPC) WithError(f LogFunc) *RPC {
	rpc.error = f
	return rpc
}

func (rpc *RPC) WithInfo(f LogFunc) *RPC {
	rpc.info = f
	return rpc
}

func (rpc *RPC) WithDebug(f LogFunc) *RPC {
	rpc.debug = f
	return rpc
}

func (rpc *RPC) WithConnection(c *amqp.Connection) *RPC {
	rpc.extConn = true
	rpc.conn = c
	return rpc
}

func (rpc *RPC) Shutdown() {
	rpc.info("Shutting down RPC")

	if rpc.outChannel != nil {
		if err := rpc.outChannel.Close(); err != nil {
			rpc.error("Publishing channel closing error %s", err.Error())
		}
		rpc.outChannel = nil
	}

	if rpc.conn != nil {
		if err := rpc.conn.Close(); err != nil {
			rpc.error("Connection closing error %s", err.Error())
		}
		rpc.conn = nil
	}
}

func (rpc RPC) publish(ch *amqp.Channel, exchange, rk string, publishing amqp.Publishing) error {
	rpc.debug("Publishing message to exchange: '%s', with routing key: '%s', reply to: '%s', correlation id: '%s'",
		exchange,
		rk,
		publishing.ReplyTo,
		publishing.CorrelationId,
	)
	return ch.Publish(
		exchange,
		rk,
		false,
		false,
		publishing,
	)
}

func (rpc RPC) subscribe(ch *amqp.Channel, queue string, handler AMQPHandler) error {
	ch.Qos(2, 0, false)
	rpc.debug("Creating subscription to queue %s", queue)
	delivery, err := ch.Consume(
		queue,
		rpc.tag,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	go func() {
		for msg := range delivery {
			rpc.debug("Received new message")
			go func(current amqp.Delivery) {
				// TODO - check if message returns to queue if code panic
				err := handler(current)
				if err != nil {
					current.Nack(false, false)
					rpc.info("Message handler error: %s", err.Error())
				} else {
					current.Ack(false)
					rpc.debug("Message processed")
				}
			}(msg)
		}
		rpc.debug("Delivery channel closed (queue %s)", queue)
	}()

	return nil
}

func (rpc *RPC) initAMQP() error {
	var err error
	rpc.conn, err = amqp.Dial(rpc.url)
	if err != nil {
		return err
	}
	rpc.debug("Created connection to AMQP ServerImpl by url %s", rpc.url)

	rpc.outChannel, err = rpc.conn.Channel()
	if err != nil {
		return err
	}
	rpc.debug("Created publishing channel")

	err = rpc.outChannel.ExchangeDeclare(
		rpc.exchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil)

	if err != nil {
		return err
	}
	rpc.debug("Exchange '%s' declared", rpc.exchangeName)

	return err
}

func exchangeName(name string) string {
	return name + ".rpc.exchange"
}

func outQueueName(serverName string) string {
	return serverName + ".out.queue"
}

func inQueueName(serverName string) string {
	return serverName + ".in.queue"
}

func requestRoutingKey(serverName, method string) string {
	return serverName + ".rpc." + method
}

func createTag(name string, server bool) string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown.host"
	}
	pid := strconv.Itoa(os.Getpid())
	role := "C"
	if server {
		role = "S"
	}
	return "[" + name + "][" + role + "][" + pid + "]" + host
}
