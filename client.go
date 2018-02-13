package rpc

import (
	"context"
	"errors"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

func (c *ClientImpl) Start() error {
	c.info("Starting client")
	var err error

	if err = c.initAMQP(); err != nil {
		return err
	}

	if err := c.subscribe(c.inChannel, c.inQueueName, c.dispatchResponse); err != nil {
		return err
	}

	return nil
}

func (c *ClientImpl) Call(ctx context.Context, method string, payload []byte, wait bool) ([]byte, error) {
	c.debug("Calling method %s", method)
	var response <-chan amqp.Delivery
	message := amqp.Publishing{}
	message.DeliveryMode = amqp.Persistent
	message.Body = payload

	if wait {
		message.ReplyTo = c.tag
		message.CorrelationId = uuid.NewV4().String()

		c.debug("Reply to: %s, correlation id: %s", message.ReplyTo, message.CorrelationId)
		defer c.removeListener(message.CorrelationId)
		response = c.addListener(message.CorrelationId)
	}

	err := c.publish(c.outChannel, c.exchangeName, requestRoutingKey(c.name, method), message)

	// return either error or just nil if response doesn't required
	if err != nil || !wait {
		return nil, err
	}

	var responseData []byte
	select {
	case responseMessage := <-response:
		c.debug("Got response from server")
		responseData = responseMessage.Body
	case <-ctx.Done():
		c.info("Got signal from context")
		err = errors.New("canceled by context")
	}

	return responseData, err
}

func (c *ClientImpl) Shutdown() {
	if c.inChannel != nil {
		err := c.inChannel.Close()
		if err != nil {
			c.error("Error while closing 'in' channel: ", err.Error())
		}
		c.inChannel = nil
	}
}

func (c *ClientImpl) initAMQP() error {
	var err error
	err = c.RPC.initAMQP()
	if err != nil {
		return err
	}

	if c.inChannel, err = c.conn.Channel(); err != nil {
		return err
	}

	if _, err = c.inChannel.QueueDeclare(c.inQueueName, true, false, true, false, nil); err != nil {
		return err
	}

	err = c.inChannel.QueueBind(
		c.inQueueName,
		c.tag,
		c.exchangeName,
		false,
		nil,
	)

	return err
}

func (c *ClientImpl) dispatchResponse(message amqp.Delivery) error {
	correlationId := message.CorrelationId
	listener, exists := c.listeners[correlationId]
	c.info("Dispatching response, correlationId: %s", correlationId)
	if exists {
		listener <- message
	} else {
		c.error("Unknown correlation id: %s", correlationId)
	}
	return nil
}

func (c *ClientImpl) addListener(correlationId string) <-chan amqp.Delivery {
	listener := make(chan amqp.Delivery)
	c.info("Registering callback listener for correlation id: %s", correlationId)
	c.listeners[correlationId] = listener
	return listener
}

func (c *ClientImpl) removeListener(correlationId string) {
	c.info("Removing listener for correlation id: %s", correlationId)
	listener := c.listeners[correlationId]
	close(listener)
	delete(c.listeners, correlationId)
}
