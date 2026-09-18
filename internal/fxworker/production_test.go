package fxworker //nolint:testpackage // unexported holders, checkers, source, and bind options

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/fx"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/httpapi"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/platform"
	"github.com/flexer2006/hopper/internal/worker"
)

type ports struct {
	fx.In

	Jobs         deliver.Jobs
	DispatchJobs dispatch.Jobs
	Source       worker.Source
	Pub          *broker.Publisher
	DispatchPub  dispatch.Publisher
	Holder       *relayHolder
	Checks       []httpapi.Checker `group:"health"`
}

type shutdownWatch struct {
	inner fx.Shutdowner
	seen  chan struct{}
}

const (
	replicaMongoURI    = "mongodb://mongo:27017/?replicaSet=rs0"
	standaloneMongoURI = "mongodb://mongo:27017"
	validAMQPURI       = "amqp://rabbitmq:5672/"
)

func (w shutdownWatch) Shutdown(opts ...fx.ShutdownOption) error {
	select {
	case w.seen <- struct{}{}:
	default:
	}

	return w.inner.Shutdown(opts...)
}

func watchShutdown(seen chan struct{}) fx.Option { //nolint:ireturn // fx.Decorate contract
	return fx.Decorate(func(inner fx.Shutdowner) fx.Shutdowner {
		return shutdownWatch{inner: inner, seen: seen}
	})
}

func writeConfig(t *testing.T, extra string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "hopper.yaml")

	err := os.WriteFile(path, []byte(platform.MinimalYAML(platform.ValidToken())+extra), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	return path
}

func aliveResources(deliveries <-chan amqp.Delivery) *workerResources {
	res := newWorkerResources()
	res.set(&amqp.Connection{}, brokerChannels{
		pub:        &amqp.Channel{},
		sub:        &amqp.Channel{},
		deliveries: deliveries,
	})

	return res
}

func checkNames(checks []httpapi.Checker) map[string]httpapi.Checker {
	byName := make(map[string]httpapi.Checker, len(checks))
	for _, item := range checks {
		byName[item.Name()] = item
	}

	return byName
}

func TestMongoOptionsLeaseMatchesClaimLease(t *testing.T) {
	t.Parallel()

	cfg := new(platform.Config)
	cfg.MongoURI = replicaMongoURI
	cfg.MongoDatabase = "hopper"
	cfg.MongoJobsCollection = "jobs"
	cfg.LeaseScanInterval = time.Second

	opts := mongoOptions(cfg)
	if opts.Lease != claimLease {
		t.Fatalf("Lease = %s, want claimLease %s (not scan interval %s)", opts.Lease, claimLease, cfg.LeaseScanInterval)
	}

	err := persist.CheckLeaseBudget(opts.Lease, httpTimeout, outcomeTimeout, confirmTimeout)
	if err != nil {
		t.Fatalf("CheckLeaseBudget(store lease) = %v", err)
	}
}

func TestBindPortsWithFakesStartStopTwice(t *testing.T) {
	t.Setenv(platform.ConfigFileEnv, writeConfig(t, ""))

	store := persist.NewMemory(nil, 0)
	res := aliveResources(make(chan amqp.Delivery))

	var got ports

	app := NewApp(
		fx.NopLogger,
		fx.Supply(store, res),
		bindPorts(),
		fx.Populate(&got),
	)

	err := app.Err()
	if err != nil {
		t.Fatalf("graph: %v", err)
	}

	if got.Jobs != store {
		t.Fatal("deliver.Jobs not bound to the supplied *persist.Store")
	}

	if got.DispatchJobs != store {
		t.Fatal("dispatch.Jobs not bound to the supplied *persist.Store")
	}

	if got.Pub == nil || got.Pub != res.pub {
		t.Fatal("*broker.Publisher must be the holder's lazy publisher (aux DLQ)")
	}

	if got.DispatchPub == nil || got.DispatchPub != res.pub {
		t.Fatal("dispatch.Publisher must be the holder's lazy publisher (ADR-001 relay)")
	}

	if got.Holder == nil || got.Holder.relay == nil {
		t.Fatal("relay holder must be started in the worker graph (FR-34 / NFR-15)")
	}

	src, ok := got.Source.(*amqpSource)
	if !ok || src.resources != res {
		t.Fatalf("worker.Source = %T, want *amqpSource over the holder", got.Source)
	}

	byName := checkNames(got.Checks)
	if len(byName) != 2 || byName["mongo"] == nil || byName["amqp"] == nil {
		t.Fatalf("health group = %v, want exactly mongo and amqp", byName)
	}

	if err = byName["amqp"].Check(t.Context()); err != nil {
		t.Fatalf("amqp check with conn + pub + sub + deliveries = %v, want nil", err)
	}

	if err = byName["mongo"].Check(t.Context()); !errors.Is(err, persist.ErrNotOpen) {
		t.Fatalf("mongo check on memory store = %v, want ErrNotOpen (health must not lie)", err)
	}

	err = platform.StartStopCycles(t.Context(), app, 2)
	if err != nil {
		t.Fatal(err)
	}
}

func TestProductionFailsFastWithoutInfrastructure(t *testing.T) {
	t.Setenv(platform.ConfigFileEnv, writeConfig(t, ""))

	err := NewApp(fx.NopLogger, Production()).Err()
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("Production without URIs err = %v, want ErrConfig", err)
	}
}

func TestProductionFailsFastOnStandaloneMongo(t *testing.T) {
	t.Setenv(platform.ConfigFileEnv, writeConfig(t, "mongo_uri: "+standaloneMongoURI+"\namqp_uri: "+validAMQPURI+"\n"))

	err := NewApp(fx.NopLogger, Production()).Err()
	if !errors.Is(err, persist.ErrStandalone) {
		t.Fatalf("Production with standalone mongo err = %v, want ErrStandalone", err)
	}
}

func TestRequireInfrastructure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		mongo string
		amqp  string
		want  error
	}{
		{name: "missing", want: platform.ErrConfig},
		{name: "standalone", mongo: standaloneMongoURI, amqp: validAMQPURI, want: persist.ErrStandalone},
		{name: "bad amqp", mongo: replicaMongoURI, amqp: "http://rabbitmq/", want: broker.ErrURI},
		{name: "ok", mongo: replicaMongoURI, amqp: validAMQPURI},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := new(platform.Config)
			cfg.MongoURI = tc.mongo
			cfg.AMQPURI = tc.amqp

			err := requireInfrastructure(cfg)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("requireInfrastructure() err = %v", err)
				}

				return
			}

			if !errors.Is(err, tc.want) {
				t.Fatalf("requireInfrastructure() err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestAMQPCheckerRequiresConsumerLiveness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		res  *workerResources
	}{
		{name: "nil resources", res: nil},
		{name: "never opened", res: newWorkerResources()},
		{name: "publisher only", res: func() *workerResources {
			res := newWorkerResources()
			res.set(&amqp.Connection{}, brokerChannels{pub: &amqp.Channel{}})

			return res
		}()},
		{name: "sub without deliveries", res: func() *workerResources {
			res := newWorkerResources()
			res.set(&amqp.Connection{}, brokerChannels{pub: &amqp.Channel{}, sub: &amqp.Channel{}})

			return res
		}()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checker := &amqpChecker{resources: tc.res}

			err := checker.Check(t.Context())
			if !errors.Is(err, broker.ErrUnavailable) {
				t.Fatalf("Check() err = %v, want ErrUnavailable", err)
			}

			if errors.Is(err, broker.ErrNoConfirm) {
				t.Fatal("health must not reuse ErrNoConfirm for a missing connection")
			}
		})
	}

	checker := &amqpChecker{resources: aliveResources(make(chan amqp.Delivery))}
	if err := checker.Check(t.Context()); err != nil {
		t.Fatalf("Check() with full consumer plumbing = %v, want nil", err)
	}
}

func TestAMQPSourceNext(t *testing.T) {
	t.Parallel()

	t.Run("nil source is unavailable", func(t *testing.T) {
		t.Parallel()

		var src *amqpSource

		_, err := src.Next(t.Context())
		if !errors.Is(err, broker.ErrUnavailable) {
			t.Fatalf("err = %v, want ErrUnavailable", err)
		}

		if errors.Is(err, broker.ErrURI) {
			t.Fatal("a closed source is not a URI problem")
		}
	})

	t.Run("before OnStart is unavailable", func(t *testing.T) {
		t.Parallel()

		src := &amqpSource{resources: newWorkerResources()}

		_, err := src.Next(t.Context())
		if !errors.Is(err, broker.ErrUnavailable) {
			t.Fatalf("err = %v, want ErrUnavailable", err)
		}
	})

	t.Run("delivery passes body through", func(t *testing.T) {
		t.Parallel()

		deliveries := make(chan amqp.Delivery, 1)
		deliveries <- amqp.Delivery{Body: []byte(`{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa"}`)}

		src := &amqpSource{resources: aliveResources(deliveries)}

		msg, err := src.Next(t.Context())
		if err != nil {
			t.Fatalf("Next() err = %v", err)
		}

		if string(msg.Body()) != `{"job_id":"aaaaaaaaaaaaaaaaaaaaaaaa"}` {
			t.Fatalf("Body() = %s", msg.Body())
		}

		if err = msg.Ack(); !errors.Is(err, broker.ErrAckChannel) {
			t.Fatalf("Ack() without acknowledger err = %v, want ErrAckChannel", err)
		}
	})

	t.Run("closed deliveries", func(t *testing.T) {
		t.Parallel()

		deliveries := make(chan amqp.Delivery)
		close(deliveries)

		src := &amqpSource{resources: aliveResources(deliveries)}

		_, err := src.Next(t.Context())
		if !errors.Is(err, errDeliveriesClosed) {
			t.Fatalf("err = %v, want errDeliveriesClosed", err)
		}
	})

	t.Run("context cancel", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		src := &amqpSource{resources: aliveResources(make(chan amqp.Delivery))}

		_, err := src.Next(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
}

func TestLazyPublisherFollowsHolder(t *testing.T) {
	t.Parallel()

	res := newWorkerResources()

	err := res.pub.PublishJob(t.Context(), broker.QueueJobs, "aaaaaaaaaaaaaaaaaaaaaaaa")
	if !errors.Is(err, broker.ErrUnavailable) {
		t.Fatalf("publish before OnStart err = %v, want ErrUnavailable", err)
	}

	pubCh := &amqp.Channel{}
	res.set(&amqp.Connection{}, brokerChannels{pub: pubCh, sub: &amqp.Channel{}, deliveries: make(chan amqp.Delivery)})

	if res.channel() != pubCh {
		t.Fatal("holder channel not visible to the lazy publisher")
	}

	conn, channels := res.release()
	if conn == nil || channels.pub != pubCh || channels.sub == nil || res.channel() != nil || res.next() != nil {
		t.Fatal("release() must hand back every handle and clear the holder")
	}

	if err = closeBroker(nil, brokerChannels{}); err != nil {
		t.Fatalf("closeBroker(nil, empty) = %v", err)
	}
}

func TestMongoCheckerNilStoreReturnsErrNotOpen(t *testing.T) {
	t.Parallel()

	checker := &mongoChecker{}

	err := checker.Check(t.Context())
	if !errors.Is(err, persist.ErrNotOpen) {
		t.Fatalf("Check() err = %v, want ErrNotOpen", err)
	}
}

func TestCheckerNamesMatchHealthRegistry(t *testing.T) {
	t.Parallel()

	var (
		mongoC mongoChecker
		amqpC  amqpChecker
	)

	if mongoC.Name() != "mongo" || amqpC.Name() != "amqp" {
		t.Fatalf("names = %q %q, want mongo amqp", mongoC.Name(), amqpC.Name())
	}
}

func TestProductionSourceKeepsConsumerPlumbing(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "production.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{"Consume": false, "PrepareWithPrefetch": false}

	ast.Inspect(file, func(node ast.Node) bool {
		sel, ok := node.(*ast.SelectorExpr)
		if ok {
			if _, tracked := seen[sel.Sel.Name]; tracked {
				seen[sel.Sel.Name] = true
			}
		}

		return true
	})

	for name, found := range seen {
		if !found {
			t.Errorf("worker graph must keep %s on the consumer channel", name)
		}
	}
}

func TestUnexpectedSourceErrorShutsDownProcess(t *testing.T) {
	t.Setenv(platform.ConfigFileEnv, writeConfig(t, ""))

	store := persist.NewMemory(nil, 0)
	closed := make(chan amqp.Delivery)
	close(closed)
	res := aliveResources(closed)
	seen := make(chan struct{}, 1)

	app := NewApp(
		fx.NopLogger,
		fx.Supply(store, res),
		bindPorts(),
		watchShutdown(seen),
	)

	err := app.Err()
	if err != nil {
		t.Fatalf("graph: %v", err)
	}

	err = app.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		stopErr := app.Stop(context.WithoutCancel(t.Context()))
		if stopErr != nil {
			t.Errorf("Stop() err = %v", stopErr)
		}
	})

	wait, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	select {
	case <-seen:
	case <-wait.Done():
		t.Fatal("Shutdowner.Shutdown was not called after the consume source closed")
	}
}
