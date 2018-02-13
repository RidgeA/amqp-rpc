package transport

import (
	"context"
)

type (
	INMemory struct {
		subscriptions map[string]SubscribeFunc
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
		t.subscriptions = make(map[string]SubscribeFunc)
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

func (t *INMemory) Subscribe(key string, f SubscribeFunc, callback bool) error {
	t.subscriptions[key] = f
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
				handler(p)
			}
		case <-t.ctx.Done():
			break
		}
	}
}
