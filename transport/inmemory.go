// In-memory implementation of transport just for testing purposes.
// Not meant to use in production

package transport

import (
	"context"
)

type (
	INMemory struct {
		subscriptions map[string]*subscription
		queue         chan *pack
		ctx           context.Context
		cancel        context.CancelFunc
		initialized   bool
	}

	pack struct {
		replyTo string
		id      string
		payload []byte
		method  string
	}

	subscription struct {
		key        string
		sFunc      SubscribeFunc
		throughput uint
		limit      chan struct{}
	}
)

func (p pack) ID() string {
	return p.id
}

func (p pack) Method() string {
	return p.method
}

func (p pack) Payload() []byte {
	return p.payload
}

func (p pack) Source() string {
	return p.replyTo
}

func NewINMemory() *INMemory {
	t := &INMemory{}
	return t
}

func (t *INMemory) Initialize() error {
	if !t.initialized {
		t.subscriptions = make(map[string]*subscription)
		t.queue = make(chan *pack, 1024)
		t.ctx, t.cancel = context.WithCancel(context.Background())
		t.initialized = true
		go t.dispatch()
	}
	return nil
}

func (t *INMemory) Shutdown() {
	t.cancel()
}

func (t *INMemory) Send(parcel Call) error {
	p := &pack{
		id:      parcel.ID(),
		replyTo: parcel.Source(),
		payload: parcel.Payload(),
		method:  parcel.Method(),
	}
	t.queue <- p
	return nil
}

func (t *INMemory) Subscribe(key string, f SubscribeFunc, throughput uint) error {

	var limitCh chan struct{}
	if throughput != 0 {
		limitCh = make(chan struct{}, throughput)
	}
	sub := &subscription{
		key:        key,
		throughput: throughput,
		sFunc:      f,
		limit:      limitCh,
	}
	t.subscriptions[key] = sub
	return nil
}

func (t *INMemory) Reply(reply Reply) error {
	p := &pack{
		method:  reply.Request().Source(),
		payload: reply.Payload(),
		id:      reply.Request().ID(),
	}
	t.queue <- p
	return nil
}

func (t *INMemory) dispatch() {
	for {
		select {
		case p := <-t.queue:
			key := p.method
			handler, exists := t.subscriptions[key]
			if exists {

				if handler.limit != nil {
					handler.limit <- struct{}{}
				}

				go func(h *subscription) {
					handler.sFunc(p)
					if handler.limit != nil {
						<-handler.limit
					}
				}(handler)

			}
		case <-t.ctx.Done():
			break
		}
	}
}
