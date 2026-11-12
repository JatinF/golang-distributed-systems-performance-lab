// Package stream runs an embedded NATS server with JetStream.
// The server listens on 127.0.0.1. A remote NATS cluster is not contacted.
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/fault"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ids"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ledger"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/store"
)

// Envelope is the JetStream body. It is not a credential.
type Envelope struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Payload []byte `json:"payload,omitempty"`
}

func Encode(id, kind string, payload []byte) ([]byte, error) {
	return json.Marshal(Envelope{ID: id, Kind: kind, Payload: payload})
}

// Bus is one embedded server, one client, and one JetStream context.
type Bus struct {
	mu  sync.Mutex
	dir string
	ns  *server.Server
	nc  *nats.Conn
	js  jetstream.JetStream
	url string
}

// Start opens a file-backed JetStream server in dir.
func Start(ctx context.Context, dir string) (*Bus, error) {
	b := &Bus{dir: dir}
	if err := b.open(ctx); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Bus) open(ctx context.Context) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	opts := &server.Options{
		ServerName: "syn-perf-lab",
		Host:       "127.0.0.1",
		Port:       port,
		NoLog:      true,
		NoSigs:     true,
		JetStream:  true,
		StoreDir:   b.dir,
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		return err
	}
	ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		return errors.New("embedded nats did not become ready")
	}
	nc, err := nats.Connect(ns.ClientURL(), nats.Name("syn-perf-lab"))
	if err != nil {
		ns.Shutdown()
		return err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return err
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       ids.Stream,
		Subjects:   []string{ids.SubjectJobs},
		Storage:    jetstream.FileStorage,
		Duplicates: time.Minute,
	})
	if err != nil {
		nc.Close()
		ns.Shutdown()
		ns.WaitForShutdown()
		return err
	}
	b.ns = ns
	b.nc = nc
	b.js = js
	b.url = ns.ClientURL()
	return nil
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	return port, ln.Close()
}

// Publish applies the fault injector, then publishes with Msg-Id set to id.
// A dropped attempt is not published. Duplicate requests a second copy with a different Msg-Id.
func (b *Bus) Publish(ctx context.Context, id string, body []byte, inj *fault.Injector) (string, error) {
	delay := time.Duration(0)
	drop, dup := false, false
	if inj != nil {
		delay, drop, dup = inj.Observe()
	}
	if drop {
		return reason.Lost, nil
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return reason.Cancelled, ctx.Err()
		}
	}
	b.mu.Lock()
	js := b.js
	b.mu.Unlock()
	if js == nil {
		return reason.MessagingUnavailable, errors.New("jetstream is not started")
	}
	ack, err := js.Publish(ctx, ids.SubjectJobs, body, jetstream.WithMsgID(id))
	if err != nil {
		return reason.MessagingUnavailable, err
	}
	if dup {
		if _, err := js.Publish(ctx, ids.SubjectJobs, body, jetstream.WithMsgID(id+"-copy")); err != nil {
			return reason.MessagingUnavailable, err
		}
		return reason.Duplicate, nil
	}
	if ack.Duplicate {
		return reason.Duplicate, nil
	}
	return reason.Done, nil
}

// Msg is one delivered JetStream message.
type Msg struct {
	raw jetstream.Msg
}

func (m Msg) Data() []byte { return m.raw.Data() }

func (m Msg) Ack() error { return m.raw.Ack() }

func (m Msg) Nak() error { return m.raw.Nak() }

func (m Msg) Deliveries() uint64 {
	meta, err := m.raw.Metadata()
	if err != nil || meta == nil {
		return 0
	}
	return meta.NumDelivered
}

func (m Msg) Envelope() (Envelope, error) {
	var env Envelope
	err := json.Unmarshal(m.Data(), &env)
	return env, err
}

// Fetch pulls up to n messages. An empty batch is not an error.
func (b *Bus) Fetch(ctx context.Context, n int, wait, ackWait time.Duration) ([]Msg, error) {
	if n < 1 {
		n = 1
	}
	if wait <= 0 {
		wait = time.Second
	}
	if ackWait <= 0 {
		ackWait = 2 * time.Second
	}
	b.mu.Lock()
	js := b.js
	b.mu.Unlock()
	if js == nil {
		return nil, errors.New("jetstream is not started")
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, ids.Stream, jetstream.ConsumerConfig{
		Durable:       ids.Durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		FilterSubject: ids.SubjectJobs,
		MaxAckPending: 1024,
	})
	if err != nil {
		return nil, err
	}
	batch, err := cons.Fetch(n, jetstream.FetchMaxWait(wait))
	if err != nil {
		return nil, err
	}
	var out []Msg
	for raw := range batch.Messages() {
		out = append(out, Msg{raw: raw})
	}
	if err := batch.Error(); err != nil && !quietFetch(err) {
		return out, err
	}
	return out, nil
}

func quietFetch(err error) bool {
	return errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, jetstream.ErrNoMessages)
}

// Apply runs the ledger, then acknowledges. A store failure is not acknowledged.
// Redelivery of a completed id is replayed. The database commit, when present,
// happens before Ack.
func Apply(ctx context.Context, m Msg, led *ledger.Ledger, st *store.Store) (model.Outcome, error) {
	env, err := m.Envelope()
	if err != nil {
		_ = m.Nak()
		return model.Outcome{Reason: reason.Rejected, State: reason.Rejected}, err
	}
	if st != nil && st.Started() {
		_, won, err := st.Complete(ctx, env.ID, ids.Tenant, env.Kind, env.Payload, "", env.ID)
		if err != nil {
			_ = m.Nak()
			return model.Outcome{ID: env.ID, State: reason.StoreUnavailable, Reason: reason.StoreUnavailable}, err
		}
		if !won {
			if err := m.Ack(); err != nil {
				return model.Outcome{ID: env.ID, Accepted: true, State: reason.Duplicate, Reason: reason.Duplicate}, err
			}
			code := reason.Duplicate
			if m.Deliveries() > 1 {
				code = reason.Replayed
			}
			return model.Outcome{ID: env.ID, Accepted: true, State: code, Reason: code}, nil
		}
	}
	out := led.Do(env.ID, func() model.Outcome {
		return model.Outcome{ID: env.ID, Accepted: true, State: reason.Done, Reason: reason.Done, Result: env.ID}
	})
	if m.Deliveries() > 1 && out.Reason == reason.Duplicate {
		out.Reason = reason.Replayed
		out.State = reason.Replayed
	}
	if out.Reason == reason.StoreUnavailable {
		_ = m.Nak()
		return out, nil
	}
	if err := m.Ack(); err != nil {
		return out, err
	}
	return out, nil
}

// Reconnect closes the client and opens another against the same server.
func (b *Bus) Reconnect(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.nc != nil {
		b.nc.Close()
	}
	nc, err := nats.Connect(b.url, nats.Name("syn-perf-lab-reconnect"))
	if err != nil {
		b.nc = nil
		b.js = nil
		return err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return err
	}
	b.nc = nc
	b.js = js
	_ = ctx
	return nil
}

// Restart stops the embedded server and starts it again on the same file store.
func (b *Bus) Restart(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.nc != nil {
		b.nc.Close()
		b.nc = nil
	}
	if b.ns != nil {
		b.ns.Shutdown()
		b.ns.WaitForShutdown()
		b.ns = nil
	}
	b.js = nil
	return b.open(ctx)
}

// Close shuts down the client and the server.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.nc != nil {
		b.nc.Close()
		b.nc = nil
	}
	if b.ns != nil {
		b.ns.Shutdown()
		b.ns.WaitForShutdown()
		b.ns = nil
	}
	b.js = nil
}
