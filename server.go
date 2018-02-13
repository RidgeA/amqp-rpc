package rpc

import (
	"github.com/streadway/amqp"
)

func (s *ServerImpl) RegisterHandler(method string, handler HandlerFunc ) {
	s.debug("Register handler for method %s", method)
	s.handlers[requestRoutingKey(s.name, method)] = &handler{
		name:    method,
		handler: handler,
	}
}

func (s *ServerImpl) Start() error {
	s.info("Starting server")

	if err := s.initAMQP(); err != nil {
		return err
	}

	var err error

	for name, handler := range s.handlers {
		err = s.subscribe(handler.channel, name, s.dispatchMessage)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *ServerImpl) Shutdown() {
	for _, handler := range s.handlers {
		err := handler.channel.Close()
		if err != nil {
			s.info("Error while closing %s channel: ", handler.name, err.Error())
		}
	}
	s.handlers = make(map[string]*handler)
	s.RPC.Shutdown()
}

func (s *ServerImpl) initAMQP() error {
	var err error

	if err := s.RPC.initAMQP(); err != nil {
		return err
	}

	for name, handler := range s.handlers {

		var err error
		if handler.channel, err = s.conn.Channel(); err != nil {
			return err
		}

		_, err = handler.channel.QueueDeclare(name, true, false, false, false, nil)
		if err != nil {
			return err
		}

		err = handler.channel.QueueBind(name, name, s.exchangeName, false, nil)
		if err != nil {
			return err
		}

	}

	return err
}

func (s *ServerImpl) dispatchMessage(delivery amqp.Delivery) error {
	s.debug("Dispatching message")
	rk := delivery.RoutingKey
	correlationId := delivery.CorrelationId
	replyTo := delivery.ReplyTo
	handler, exists := s.handlers[rk]
	s.debug("Routing key: %s, reply to: %s, correlation id: %s", rk, replyTo, correlationId)

	if !exists {
		s.error("Can't find handler for routing key %s", rk)
		return nil
	}

	data, err := handler.handler(delivery.Body)

	if err != nil {
		return err
	}

	if data != nil {
		err = s.response(replyTo, correlationId, data)
	}

	return err
}

func (s *ServerImpl) response(rk, correlationId string, data []byte) error {

	if correlationId == "" {
		return nil
	}

	s.debug("Sending result to exchange %s, roting key: %s, correlation id: %s",
		s.exchangeName,
		rk,
		correlationId)

	return s.publish(s.outChannel, s.exchangeName, rk, amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		CorrelationId: correlationId,
		Body:          data,
	})
}
