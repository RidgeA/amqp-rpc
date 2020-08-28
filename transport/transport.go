package transport

type (
	Call interface {
		ID() string
		Method() string
		Payload() []byte
		Source() string
	}

	Reply interface {
		Request() Call
		Payload() []byte
	}

	SubscribeFunc func(Call) error
)
