package transport

import (
	"github.com/streadway/amqp"
	"strings"
)

type (
	AMQPTransport struct {
		url          string
		name         string
		tag          string
		exchangeName string
		extConn      bool
		conn         *amqp.Connection
		out          *amqp.Channel
		inChannels   map[string]*amqp.Channel
		initialized  chan struct{}
	}

	OptionsFunc func(transport *AMQPTransport)

	AMQPParcel amqp.Delivery
)

func (p AMQPParcel) Method() string {
	index := strings.LastIndex(p.RoutingKey, ".")
	return p.RoutingKey[index:]
}

func (p AMQPParcel) Source() string {
	return p.ReplyTo
}

func (p AMQPParcel) ID() string {
	return p.CorrelationId
}

func (p AMQPParcel) Payload() []byte {
	return p.Body
}

func NewAMQPTransport(name, id, url string, options ...OptionsFunc) *AMQPTransport {
	t := &AMQPTransport{
		url:          url,
		name:         name,
		exchangeName: exchangeName(name),
		tag:          id,
	}

	for _, f := range options {
		f(t)
	}

	t.inChannels = make(map[string]*amqp.Channel)
	t.initialized = make(chan struct{})
	return t
}

func SetConnection(conn *amqp.Connection) OptionsFunc {
	return func(t *AMQPTransport) {
		t.extConn = true
		t.conn = conn
	}
}

func (t *AMQPTransport) Initialize() error {
	var err error
	defer close(t.initialized)

	t.conn, err = amqp.Dial(t.url)
	if err != nil {
		return err
	}

	t.out, err = t.conn.Channel()
	if err != nil {
		return err
	}

	err = t.out.ExchangeDeclare(t.exchangeName, "direct", false, true, false, false, nil)

	return err
}

func (t *AMQPTransport) Shutdown() {
	if err := t.out.Close(); err != nil {
		//todo log
	}

	if !t.extConn {
		if err := t.conn.Close(); err != nil {
			//todo log
		}
	}
}

func (t *AMQPTransport) Send(p Call) error {
	rk := requestQueue(t.name, p.Method())
	publishing := amqp.Publishing{
		ReplyTo:       requestQueue(t.name, t.tag),
		DeliveryMode:  amqp.Persistent,
		CorrelationId: p.ID(),
		Body:          p.Payload(),
	}
	return t.out.Publish(t.exchangeName, rk, true, false, publishing)
}

func (t *AMQPTransport) Reply(reply Reply) error {
	publishing := amqp.Publishing{
		ReplyTo:       t.tag,
		DeliveryMode:  amqp.Persistent,
		CorrelationId: reply.Request().ID(),
		Body:          reply.Payload(),
	}
	return t.out.Publish(t.exchangeName, reply.Request().Source(), true, false, publishing)
}

func (t *AMQPTransport) Subscribe(method string, subscription SubscribeFunc, throughput uint) error {
	<-t.initialized
	ch, err := t.getSubscribeChannel(method)
	if err != nil {
		return err
	}

	queueName := requestQueue(t.name, method)

	err = t.ensureQueue(ch, queueName)
	if err != nil {
		return err
	}

	if throughput > 0 {
		ch.Qos(int(throughput), 0, false)
	}

	delivery, err := ch.Consume(queueName, t.tag, false, false, false, false, nil)

	go t.handle(delivery, subscription)

	return nil
}

func (t *AMQPTransport) handle(in <-chan amqp.Delivery, f SubscribeFunc) {
	for msg := range in {
		err := f(AMQPParcel(msg))
		if err != nil {
			msg.Nack(false, false)
		} else {
			msg.Ack(false)
		}
	}
}

func (t *AMQPTransport) getSubscribeChannel(key string) (*amqp.Channel, error) {
	var err error
	_, exists := t.inChannels[key]
	if !exists {
		t.inChannels[key], err = t.conn.Channel()
	}
	return t.inChannels[key], err
}

func (t *AMQPTransport) ensureQueue(channel *amqp.Channel, name string) error {
	_, err := channel.QueueDeclare(name, false, true, false, false, nil)
	if err != nil {
		return nil
	}
	err = channel.QueueBind(name, name, t.exchangeName, false, nil)
	return err
}

func exchangeName(name string) string {
	return name + ".rpc.exchange"
}

func requestQueue(name, method string) string {
	return name + ".rpc." + method
}
