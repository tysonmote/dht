package dht

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stvp/rendezvous"
)

const (
	checkInterval = 5 * time.Second
	// errorRetryWait limits how fast we retry Consul after a failed update when
	// the blocking query returns an error (for example a temporary disconnect).
	errorRetryWait = 200 * time.Millisecond
)

// JoinOption configures Join and JoinContext.
type JoinOption func(*joinOptions)

type joinOptions struct {
	logger *log.Logger
}

// WithLogger causes the node to write background poll errors to l instead of
// the standard log package.
func WithLogger(l *log.Logger) JoinOption {
	return func(o *joinOptions) {
		o.logger = l
	}
}

func newCheckListenerAndServer() (listener net.Listener, server *http.Server, err error) {
	listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}

	server = &http.Server{
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		Handler: http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
			fmt.Fprintf(resp, "OK")
		}),
	}

	// When the listener is closed, this goroutine returns.
	go server.Serve(listener)

	return listener, server, nil
}

// Node is a single node in a distributed hash table, coordinated using
// services registered in Consul. Key membership is determined using rendezvous
// hashing to ensure even distribution of keys and minimal key membership
// changes when a Node fails or otherwise leaves the hash table.
//
// Errors encountered when making blocking GET requests to the Consul agent API
// are logged using the log package, or the logger passed via WithLogger.
//
// Member is safe to call from multiple goroutines concurrently with the
// background poll that refreshes table state.
type Node struct {
	// Consul
	serviceName string
	serviceID   string
	consul      *api.Client

	// HTTP health check server
	checkListener net.Listener
	checkServer   *http.Server

	// Hash table (guarded by hashMu)
	hashMu    sync.RWMutex
	hashTable *rendezvous.Table
	waitIndex uint64

	// Graceful shutdown
	stop      chan struct{}
	leaveOnce sync.Once

	logger *log.Logger
}

// Join creates a new Node and adds it to the distributed hash table specified
// by the given name. The given id should be unique among all Nodes in the hash
// table. It is equivalent to JoinContext(context.Background(), name, id).
func Join(name, id string, opts ...JoinOption) (node *Node, err error) {
	return JoinContext(context.Background(), name, id, opts...)
}

// JoinContext is like Join but uses ctx for the initial service registration
// and the first blocking fetch of healthy instances. If ctx is canceled or times
// out before JoinContext returns, partial setup is rolled back. After a
// successful return, the background poll is not tied to ctx; use Leave to stop
// the node.
func JoinContext(ctx context.Context, name, id string, opts ...JoinOption) (node *Node, err error) {
	var jo joinOptions
	for _, opt := range opts {
		opt(&jo)
	}

	node = &Node{
		logger:      jo.logger,
		serviceName: name,
		serviceID:   id,
		stop:        make(chan struct{}),
	}

	node.consul, err = api.NewClient(api.DefaultConfig())
	if err != nil {
		return nil, fmt.Errorf("dht: can't create Consul API client: %s", err)
	}

	node.checkListener, node.checkServer, err = newCheckListenerAndServer()
	if err != nil {
		return nil, fmt.Errorf("dht: can't start HTTP server: %s", err)
	}

	if err = node.register(ctx); err != nil {
		node.closeCheckServer(true)
		return nil, fmt.Errorf("dht: can't register %s service: %s", node.serviceName, err)
	}

	if err = node.update(ctx); err != nil {
		_ = node.consul.Agent().ServiceDeregister(node.serviceID)
		node.closeCheckServer(true)
		return nil, fmt.Errorf("dht: can't fetch %s services list: %s", node.serviceName, err)
	}

	go node.poll()

	return node, nil
}

// closeCheckServer stops the health check HTTP server. If force is true, the
// listener is closed immediately (used when Join fails before any traffic).
func (n *Node) closeCheckServer(force bool) {
	if n.checkServer == nil {
		return
	}
	if force {
		if n.checkListener != nil {
			_ = n.checkListener.Close()
		}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = n.checkServer.Shutdown(ctx)
		cancel()
		// Shutdown closes the listener.
	}
	n.checkListener = nil
	n.checkServer = nil
}

func (n *Node) register(ctx context.Context) error {
	return n.consul.Agent().ServiceRegisterOpts(&api.AgentServiceRegistration{
		Name: n.serviceName,
		ID:   n.serviceID,
		Check: &api.AgentServiceCheck{
			HTTP:     fmt.Sprintf("http://%s", n.checkListener.Addr().String()),
			Interval: checkInterval.String(),
		},
	}, api.ServiceRegisterOpts{}.WithContext(ctx))
}

func (n *Node) poll() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-n.stop
		cancel()
	}()

	for {
		err := n.update(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			n.logf("[dht %s %s] error: %s", n.serviceName, n.serviceID, err)
			select {
			case <-n.stop:
				return
			case <-ctx.Done():
				return
			case <-time.After(errorRetryWait):
			}
			continue
		}
	}
}

// update blocks until the service list changes or until the Consul agent's
// timeout is reached (10 minutes by default).
func (n *Node) update(ctx context.Context) error {
	n.hashMu.RLock()
	wi := n.waitIndex
	n.hashMu.RUnlock()

	opts := (&api.QueryOptions{WaitIndex: wi}).WithContext(ctx)
	serviceEntries, meta, err := n.consul.Health().Service(n.serviceName, "", true, opts)
	if err != nil {
		return err
	}

	ids := make([]string, len(serviceEntries))
	for i, entry := range serviceEntries {
		ids[i] = entry.Service.ID
	}

	table := rendezvous.New(ids)

	n.hashMu.Lock()
	n.hashTable = table
	n.waitIndex = meta.LastIndex
	n.hashMu.Unlock()

	return nil
}

// Member returns true if the given key belongs to this Node in the distributed
// hash table. If the table is not yet initialized, it returns false.
func (n *Node) Member(key string) bool {
	n.hashMu.RLock()
	defer n.hashMu.RUnlock()
	if n.hashTable == nil {
		return false
	}
	return n.hashTable.Get(key) == n.serviceID
}

// Leave removes the Node from the distributed hash table by de-registering it
// from Consul. It is equivalent to LeaveContext(context.Background()).
func (n *Node) Leave() error {
	return n.LeaveContext(context.Background())
}

// LeaveContext is like Leave but uses ctx for the Consul deregistration request.
// It is safe to call more than once; only the first call performs work.
//
// If deregistration fails on the first call, that error is returned and later
// calls return nil.
func (n *Node) LeaveContext(ctx context.Context) (leaveErr error) {
	n.leaveOnce.Do(func() {
		close(n.stop)

		if err := n.consul.Agent().ServiceDeregisterOpts(n.serviceID, (&api.QueryOptions{}).WithContext(ctx)); err != nil {
			leaveErr = err
		}
		n.closeCheckServer(false)
	})
	return leaveErr
}

func (n *Node) logf(format string, v ...interface{}) {
	if n.logger != nil {
		n.logger.Printf(format, v...)
		return
	}
	log.Printf(format, v...)
}

// refresh resets the Consul blocking index and runs one immediate update. It is
// used by tests and is safe to call while the background poll loop is running.
func (n *Node) refresh(ctx context.Context) error {
	n.hashMu.Lock()
	n.waitIndex = 0
	n.hashMu.Unlock()
	return n.update(ctx)
}
