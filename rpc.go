// Simple RPC library with amqp as default underlying transport
package rpc

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"

	"github.com/satori/go.uuid"
	"github.com/RidgeA/amqp-rpc/transport"
)

var (
	silentLog = func(format string, args ...interface{}) {}
	errorLog  = log.Printf
)

// Instance mode: Client, Server or both
const (
	ModeClient = 1 << iota
	ModeServer
	ModeDuplex = ModeClient | ModeServer
)

type (
	//Interface that represents rpc server
	Server interface {
		Start() error
		Shutdown()
		RegisterHandler(string, HandlerFunc, ...HandlerOptionsFunc)
	}

	//Interface that represents rpc client
	Client interface {
		Start() error
		Shutdown()
		Call(context.Context, string, []byte, bool) ([]byte, error)
	}

	//Interface that represents rpc client and server
	Duplex interface {
		Start() error
		Shutdown()
		Call(context.Context, string, []byte, bool) ([]byte, error)
		RegisterHandler(string, HandlerFunc, ...HandlerOptionsFunc)
	}

	//Interface for transport implementation
	Transport interface {
		Initialize() error
		Shutdown()
		Send(call transport.Call) error
		Subscribe(key string, subscription transport.SubscribeFunc, throughput uint) error
		Reply(transport.Reply) error
	}

	//Logger function
	LogFunc func(string, ...interface{})

	HandlerFunc func([]byte) ([]byte, error)

	//Function type to set options for RPC instance
	OptionsFunc func(*rpc)

	//Function type to set options for handler
	HandlerOptionsFunc func(*handler)

	handler struct {
		method     string
		handler    HandlerFunc
		throughput uint
	}

	request struct {
		id      string
		payload []byte
		source  string
		method  string
	}

	response struct {
		req     transport.Call
		payload []byte
	}

	rpc struct {
		errorf, info, debug LogFunc
		extTransport        bool
		t                   Transport
		instanceId          string
		mode                int
		url                 string
		name                string
		listeners           map[string]chan transport.Call
		handlers            map[string]*handler
	}
)

func (r response) Request() transport.Call {
	return r.req
}

func (r response) Payload() []byte {
	return r.payload
}

func (p request) ID() string {
	return p.id
}

func (p request) Payload() []byte {
	return p.payload
}

func (p request) Source() string {
	return p.source
}

func (p request) Method() string {
	return p.method
}

//Creates new server instance with 'name'. Optional params could be configured via option functions
//The name has to be the same across whole infrastructure
//Either url to ampq server or transport has to be configured via options
func NewServer(name string, opts ...OptionsFunc) Server {
	r := newRPC(name, opts...)
	r.mode = ModeServer
	r.handlers = make(map[string]*handler)
	return r
}

//Creates new client instance with 'name'. Optional params could be configured via option functions
//The name has to be the same across whole infrastructure
//Either url to ampq server or transport has to be configured via options
func NewClient(name string, opts ...OptionsFunc) Client {
	r := newRPC(name, opts...)
	r.mode = ModeClient
	r.listeners = make(map[string]chan transport.Call)
	return r
}

//Creates new RPC instance with 'name' that could act both as server and client.
//The name has to be the same across whole infrastructure
//Optional params could be configured via option functions
//Either url to ampq server or transport has to be configured via options
func NewDuplex(name string, opts ...OptionsFunc) Duplex {
	r := newRPC(name, opts...)
	r.mode = ModeDuplex
	r.listeners = make(map[string]chan transport.Call)
	r.handlers = make(map[string]*handler)
	return r
}

//Sets function to log errors
func SetError(f LogFunc) OptionsFunc {
	return func(r *rpc) {
		r.errorf = f
	}
}

//Sets function to log informational messages
func SetInfo(f LogFunc) OptionsFunc {
	return func(r *rpc) {
		r.info = f
	}
}

//Sets function to log debug messages
func SetDebug(f LogFunc) OptionsFunc {
	return func(r *rpc) {
		r.debug = f
	}
}

//Sets URL to amqp server
func SetUrl(url string) OptionsFunc {
	return func(r *rpc) {
		r.url = url
	}
}

//Sets already configured transport instance
//Transport implementation has to be initialized and destroyed manually
func SetTransport(transport Transport) OptionsFunc {
	return func(r *rpc) {
		r.extTransport = true
		r.t = transport
	}
}

// Sets optional options for method handler to limit throughput
// Limitation passes to underlying transport and has to be implemented by transport
func SetHandlerThroughput(throughput uint) HandlerOptionsFunc {
	return func(h *handler) {
		h.throughput = throughput
	}
}

//Method to start RPC instance
//Depends on selected mode it initialize server, client or both.
//If transport instance wasn't passed manually it also initializes the default transport (amqp)
func (rpc *rpc) Start() error {

	if !rpc.extTransport {
		if err := rpc.t.Initialize(); err != nil {
			return err
		}
	}

	if rpc.mode&ModeClient == ModeClient {
		if err := rpc.startClient(); err != nil {
			return err
		}
	}

	if rpc.mode&ModeServer == ModeServer {
		if err := rpc.startServer(); err != nil {
			return err
		}
	}

	return nil
}

//Method for shutdown instance properly and close underlying transport instance
//The method doesn't close transport implementation if it has been passed manually
func (rpc *rpc) Shutdown() {
	rpc.info("Shutting down rpc")
	if !rpc.extTransport {
		rpc.t.Shutdown()
	}
}

//Method to call remote function
func (rpc *rpc) Call(ctx context.Context, method string, payload []byte, wait bool) ([]byte, error) {
	rpc.debug("Calling method %s", method)
	var response <-chan transport.Call
	p := request{
		id:      uuid.NewV4().String(),
		payload: payload,
		method:  method,
		source:  rpc.instanceId,
	}

	if wait {
		defer rpc.removeListener(p.id)
		response = rpc.addListener(p.id)
	}

	err := rpc.t.Send(p)

	// return either errorf or just nil if response doesn't required
	if err != nil || !wait {
		return nil, err
	}

	var responseData []byte
	select {
	case responseMessage := <-response:
		rpc.debug("Got response from server")
		responseData = responseMessage.Payload()
	case <-ctx.Done():
		rpc.info("Got signal from context")
		err = errors.New("canceled by context")
	}

	return responseData, err
}

//Method to register handler for the method
func (rpc *rpc) RegisterHandler(method string, f HandlerFunc, options ...HandlerOptionsFunc) {
	rpc.debug("Register handler for method %rpc", method)
	h := &handler{
		method:  method,
		handler: f,
	}
	for _, setter := range options {
		setter(h)
	}
	rpc.handlers[method] = h
}

func newRPC(name string, opts ...OptionsFunc) (r *rpc) {
	r = new(rpc)
	r.name = name
	r.errorf = errorLog
	r.info = silentLog
	r.debug = silentLog
	r.instanceId = r.createInstanceId()

	for _, setter := range opts {
		setter(r)
	}

	if r.t == nil {
		r.t = transport.NewAMQPTransport(
			r.name,
			r.instanceId,
			r.url,
		)
	}
	return
}

func (rpc *rpc) handle(f HandlerFunc) transport.SubscribeFunc {

	return func(p transport.Call) error {
		responsePayload, err := f(p.Payload())
		if err != nil {
			return err
		}

		if responsePayload != nil {
			response := response{
				req:     p,
				payload: responsePayload,
			}
			err = rpc.t.Reply(response)

		}
		return nil
	}
}

func (rpc *rpc) startClient() error {
	rpc.info("Starting client")
	if err := rpc.t.Subscribe(rpc.instanceId, rpc.dispatchResponse, 0); err != nil {
		return err
	}
	return nil
}

func (rpc *rpc) startServer() error {

	rpc.info("Starting server")

	var err error

	for _, handler := range rpc.handlers {
		err = rpc.t.Subscribe(handler.method, rpc.handle(handler.handler), handler.throughput)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rpc *rpc) createInstanceId() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown.host"
	}
	pid := strconv.Itoa(os.Getpid())
	return rpc.name + "." + pid + "." + host
}

func (rpc *rpc) dispatchResponse(message transport.Call) error {
	id := message.ID()
	listener, exists := rpc.listeners[id]
	rpc.info("Dispatching response, id: %s", id)
	if exists {
		listener <- message
	} else {
		rpc.errorf("Unknown id: %s", id)
	}
	return nil
}

func (rpc *rpc) addListener(id string) <-chan transport.Call {
	listener := make(chan transport.Call)
	rpc.info("Registering callback listener for id: %s", id)
	rpc.listeners[id] = listener
	return listener
}

func (rpc *rpc) removeListener(id string) {
	rpc.info("Removing listener for id: %s", id)
	listener, exists := rpc.listeners[id]
	if exists {
		close(listener)
		delete(rpc.listeners, id)
	}
}
