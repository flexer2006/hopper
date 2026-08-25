package broker_test

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/goleak"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/domain"
)

type exchangeCall struct {
	args                amqp.Table
	name                string
	kind                string
	durable, autoDelete bool
	internal, noWait    bool
}

type queueCall struct {
	args                                   amqp.Table
	name                                   string
	durable, autoDelete, exclusive, noWait bool
}

type bindCall struct {
	name, key, exchange string
	noWait              bool
}

type fakeTopo struct {
	confirmErr  error
	exchangeErr error
	bindErr     error
	qosErr      error
	queueErr    error
	exchanges   []exchangeCall
	queues      []queueCall
	binds       []bindCall
	qosCount    int
	qosSize     int
	qosGlobal   bool
	confirmed   bool
	confirmWait bool
}

func (fake *fakeTopo) Confirm(noWait bool) error {
	fake.confirmed = true
	fake.confirmWait = !noWait

	return fake.confirmErr
}

func (fake *fakeTopo) ExchangeDeclare(
	name, kind string,
	durable, autoDelete, internal, noWait bool,
	args amqp.Table,
) error {
	fake.exchanges = append(fake.exchanges, exchangeCall{
		name:       name,
		kind:       kind,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		noWait:     noWait,
		args:       args,
	})

	return fake.exchangeErr
}

func (fake *fakeTopo) QueueDeclare(
	name string,
	durable, autoDelete, exclusive, noWait bool,
	args amqp.Table,
) (amqp.Queue, error) {
	fake.queues = append(fake.queues, queueCall{
		name:       name,
		durable:    durable,
		autoDelete: autoDelete,
		exclusive:  exclusive,
		noWait:     noWait,
		args:       args,
	})

	return amqp.Queue{Name: name}, fake.queueErr
}

func (fake *fakeTopo) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	fake.binds = append(fake.binds, bindCall{
		name:     name,
		key:      key,
		exchange: exchange,
		noWait:   noWait,
	})
	_ = args

	return fake.bindErr
}

func (fake *fakeTopo) Qos(prefetchCount, prefetchSize int, global bool) error {
	fake.qosCount = prefetchCount
	fake.qosSize = prefetchSize
	fake.qosGlobal = global

	return fake.qosErr
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestDeclareClassicDurableTTLAndDLRK(t *testing.T) {
	t.Parallel()

	fake := &fakeTopo{}

	err := broker.Declare(fake)
	if err != nil {
		t.Fatalf("Declare: %v", err)
	}

	if len(fake.exchanges) != 1 {
		t.Fatalf("exchanges = %d, want 1", len(fake.exchanges))
	}

	exch := fake.exchanges[0]
	if exch.name != broker.ExchangeDelayDLX || exch.kind != "direct" || !exch.durable || exch.autoDelete ||
		exch.internal {
		t.Fatalf("exchange = %+v", exch)
	}

	names, err := broker.AllQueueNames()
	if err != nil {
		t.Fatalf("AllQueueNames: %v", err)
	}

	if len(fake.queues) != len(names) {
		t.Fatalf("queues = %d, want %d", len(fake.queues), len(names))
	}

	byName := make(map[string]queueCall, len(fake.queues))
	for _, queued := range fake.queues {
		if !queued.durable || queued.autoDelete || queued.exclusive {
			t.Fatalf("queue %s not classic durable: %+v", queued.name, queued)
		}

		byName[queued.name] = queued
	}

	jobs, ok := byName[broker.QueueJobs]
	if !ok || jobs.args != nil {
		t.Fatalf("jobs queue args = %v", jobs.args)
	}

	dlq, ok := byName[domain.QueueDLQ]
	if !ok || dlq.args != nil {
		t.Fatalf("dlq queue args = %v", dlq.args)
	}

	delays, err := broker.DelayQueues()
	if err != nil {
		t.Fatalf("DelayQueues: %v", err)
	}

	wantTTL := map[string]int32{
		"jobs.delay.1s":  1000,
		"jobs.delay.2s":  2000,
		"jobs.delay.4s":  4000,
		"jobs.delay.8s":  8000,
		"jobs.delay.16s": 16000,
		"jobs.delay.32s": 32000,
		"jobs.delay.60s": 60000,
	}

	if len(delays) != len(wantTTL) {
		t.Fatalf("delay queues = %v", delays)
	}

	for _, name := range delays {
		queued, exists := byName[name]
		if !exists {
			t.Fatalf("missing delay queue %s", name)
		}

		ttl, ttlOK := queued.args[amqp.QueueMessageTTLArg].(int32)
		if !ttlOK || ttl != wantTTL[name] {
			t.Fatalf(
				"%s x-message-ttl = %v (%T), want %d",
				name,
				queued.args[amqp.QueueMessageTTLArg],
				queued.args[amqp.QueueMessageTTLArg],
				wantTTL[name],
			)
		}

		if queued.args["x-dead-letter-exchange"] != broker.ExchangeDelayDLX {
			t.Fatalf("%s dlx = %v", name, queued.args["x-dead-letter-exchange"])
		}

		if queued.args["x-dead-letter-routing-key"] != broker.RoutingKeyJobs {
			t.Fatalf("%s dlrk = %v, want jobs (AT-INT-01 / REL-03)", name, queued.args["x-dead-letter-routing-key"])
		}

		if _, hasExp := queued.args["x-expires"]; hasExp {
			t.Fatalf("%s must not set x-expires", name)
		}
	}

	if len(fake.binds) != 1 {
		t.Fatalf("binds = %+v", fake.binds)
	}

	bind := fake.binds[0]
	if bind.name != broker.QueueJobs || bind.key != broker.RoutingKeyJobs || bind.exchange != broker.ExchangeDelayDLX {
		t.Fatalf("bind = %+v", bind)
	}
}

func TestTTLMillisMatchesSecondsTimes1000(t *testing.T) {
	t.Parallel()

	if got := broker.TTLMillis(1); got != 1000 {
		t.Fatalf("TTLMillis(1) = %d", got)
	}

	if got := broker.TTLMillis(60); got != 60000 {
		t.Fatalf("TTLMillis(60) = %d", got)
	}

	if got := broker.TTLMillis(-1); got != 0 {
		t.Fatalf("TTLMillis(-1) = %d", got)
	}
}

func TestPrepareConfirmDeclarePrefetch1(t *testing.T) {
	t.Parallel()

	fake := &fakeTopo{}

	err := broker.Prepare(fake, fake)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	if !fake.confirmed || !fake.confirmWait {
		t.Fatalf("confirm.select noWait=false required, got confirmed=%v wait=%v", fake.confirmed, fake.confirmWait)
	}

	if fake.qosCount != broker.PrefetchCount || fake.qosSize != 0 || fake.qosGlobal {
		t.Fatalf("qos = count=%d size=%d global=%v, want prefetch 1", fake.qosCount, fake.qosSize, fake.qosGlobal)
	}
}

func TestPrepareErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("confirm", func(t *testing.T) {
		t.Parallel()

		want := errors.New("confirm")
		fake := &fakeTopo{confirmErr: want}

		err := broker.Prepare(fake, fake)
		if !errors.Is(err, want) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("exchange", func(t *testing.T) {
		t.Parallel()

		want := errors.New("exchange")
		fake := &fakeTopo{exchangeErr: want}

		err := broker.Prepare(fake, fake)
		if !errors.Is(err, want) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("qos", func(t *testing.T) {
		t.Parallel()

		want := errors.New("qos")
		fake := &fakeTopo{qosErr: want}

		err := broker.Prepare(fake, fake)
		if !errors.Is(err, want) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestDeclareQueueAndBindErrors(t *testing.T) {
	t.Parallel()

	t.Run("queue", func(t *testing.T) {
		t.Parallel()

		want := errors.New("queue")
		fake := &fakeTopo{queueErr: want}

		err := broker.Declare(fake)
		if !errors.Is(err, want) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("bind", func(t *testing.T) {
		t.Parallel()

		want := errors.New("bind")
		fake := &fakeTopo{bindErr: want}

		err := broker.Declare(fake)
		if !errors.Is(err, want) {
			t.Fatalf("err = %v", err)
		}
	})
}
