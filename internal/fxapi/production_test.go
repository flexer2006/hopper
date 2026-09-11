package fxapi //nolint:testpackage // unexported holders, checkers, and bind options

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/fx"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/dispatch"
	"github.com/flexer2006/hopper/internal/enqueue"
	"github.com/flexer2006/hopper/internal/httpapi"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/platform"
	"github.com/flexer2006/hopper/internal/query"
	"github.com/flexer2006/hopper/internal/replay"
)

type ports struct {
	fx.In

	Jobs      dispatch.Jobs
	Enqueue   enqueue.Store
	Query     query.Store
	Replay    replay.Store
	Publisher dispatch.Publisher
	EnqSvc    *enqueue.Service
	QrySvc    *query.Service
	RplSvc    *replay.Service
	Holder    *relayHolder
	Checks    []httpapi.Checker `group:"health"`
}

const (
	replicaMongoURI    = "mongodb://mongo:27017/?replicaSet=rs0"
	standaloneMongoURI = "mongodb://mongo:27017"
	validAMQPURI       = "amqp://rabbitmq:5672/"
	loopbackAddr       = "127.0.0.1:0"
)

func writeConfig(t *testing.T, extra string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "hopper.yaml")

	err := os.WriteFile(path, []byte(platform.MinimalYAML(platform.ValidToken())+extra), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	return path
}

func aliveResources() *brokerResources {
	res := newBrokerResources()
	res.set(&amqp.Connection{}, &amqp.Channel{})

	return res
}

func checkNames(checks []httpapi.Checker) map[string]httpapi.Checker {
	byName := make(map[string]httpapi.Checker, len(checks))
	for _, item := range checks {
		byName[item.Name()] = item
	}

	return byName
}

func TestMongoOptionsLeaseIsNotScanInterval(t *testing.T) {
	t.Parallel()

	cfg := new(platform.Config)
	cfg.MongoURI = replicaMongoURI
	cfg.MongoDatabase = "hopper"
	cfg.MongoJobsCollection = "jobs"
	cfg.LeaseScanInterval = 5 * time.Second

	opts := mongoOptions(cfg)
	if opts.Lease != platform.DefaultClaimLease {
		t.Fatalf(
			"Lease = %s, want claim TTL %s (not scan interval %s)",
			opts.Lease,
			platform.DefaultClaimLease,
			cfg.LeaseScanInterval,
		)
	}

	if opts.URI != replicaMongoURI || opts.Database != "hopper" || opts.Collection != "jobs" {
		t.Fatalf("mongoOptions() = %+v", opts)
	}
}

func TestBindPortsWithFakesStartStopTwice(t *testing.T) {
	t.Setenv(platform.ConfigFileEnv, writeConfig(t, ""))
	t.Setenv(platform.HTTPAddrEnv, loopbackAddr)

	store := persist.NewMemory(nil, 0)
	res := aliveResources()

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

	if got.Jobs != store || got.Enqueue != store || got.Query != store || got.Replay != store {
		t.Fatalf("store ports not bound to the supplied *persist.Store: %+v", got)
	}

	if got.Publisher == nil || got.Publisher != res.pub {
		t.Fatal("dispatch.Publisher must be the holder's lazy publisher")
	}

	if got.EnqSvc == nil || got.QrySvc == nil || got.RplSvc == nil {
		t.Fatalf("services nil: enqueue=%v query=%v replay=%v", got.EnqSvc, got.QrySvc, got.RplSvc)
	}

	if got.Holder == nil || got.Holder.relay == nil {
		t.Fatal("relay holder empty although Jobs and Publisher are bound")
	}

	byName := checkNames(got.Checks)
	if len(byName) != 2 || byName["mongo"] == nil || byName["amqp"] == nil {
		t.Fatalf("health group = %v, want exactly mongo and amqp", byName)
	}

	if err = byName["amqp"].Check(t.Context()); err != nil {
		t.Fatalf("amqp check with live handles = %v, want nil", err)
	}

	if err = byName["mongo"].Check(t.Context()); !errors.Is(err, persist.ErrNotOpen) {
		t.Fatalf("mongo check on memory store = %v, want ErrNotOpen (health must not lie)", err)
	}

	for cycle := range 2 {
		err = platform.StartStop(t.Context(), app)
		if err != nil {
			t.Fatalf("cycle %d start/stop: %v", cycle, err)
		}
	}
}

func TestProductionFailsFastWithoutInfrastructure(t *testing.T) {
	t.Setenv(platform.ConfigFileEnv, writeConfig(t, ""))
	t.Setenv(platform.HTTPAddrEnv, loopbackAddr)

	err := NewApp(fx.NopLogger, Production()).Err()
	if !errors.Is(err, platform.ErrConfig) {
		t.Fatalf("Production without URIs err = %v, want ErrConfig", err)
	}
}

func TestProductionFailsFastOnStandaloneMongo(t *testing.T) {
	t.Setenv(platform.ConfigFileEnv, writeConfig(t, "mongo_uri: "+standaloneMongoURI+"\namqp_uri: "+validAMQPURI+"\n"))
	t.Setenv(platform.HTTPAddrEnv, loopbackAddr)

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

func TestAMQPCheckerReportsUnavailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		res  *brokerResources
	}{
		{name: "nil resources", res: nil},
		{name: "never opened", res: newBrokerResources()},
		{name: "conn only", res: func() *brokerResources {
			res := newBrokerResources()
			res.set(&amqp.Connection{}, nil)

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

	checker := &amqpChecker{resources: aliveResources()}
	if err := checker.Check(t.Context()); err != nil {
		t.Fatalf("Check() with conn + pub channel = %v, want nil", err)
	}
}

func TestLazyPublisherFollowsHolder(t *testing.T) {
	t.Parallel()

	res := newBrokerResources()

	err := res.pub.PublishJob(t.Context(), broker.QueueJobs, "aaaaaaaaaaaaaaaaaaaaaaaa")
	if !errors.Is(err, broker.ErrUnavailable) {
		t.Fatalf("publish before OnStart err = %v, want ErrUnavailable", err)
	}

	pubCh := &amqp.Channel{}
	res.set(&amqp.Connection{}, pubCh)

	if res.channel() != pubCh {
		t.Fatal("holder channel not visible to the lazy publisher")
	}

	conn, gotCh := res.release()
	if conn == nil || gotCh != pubCh || res.channel() != nil {
		t.Fatal("release() must hand back both handles and clear the holder")
	}

	if err = closeBroker(nil, nil); err != nil {
		t.Fatalf("closeBroker(nil, nil) = %v", err)
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

func TestBrokerResourcesHoldOnlyPublisherChannel(t *testing.T) {
	t.Parallel()

	channelType := reflect.TypeFor[*amqp.Channel]()
	deliveriesType := reflect.TypeFor[<-chan amqp.Delivery]()
	channels := 0

	for field := range reflect.TypeFor[brokerResources]().Fields() {
		if field.Type == deliveriesType {
			t.Fatalf("API holder retains a deliveries channel via field %s", field.Name)
		}

		if field.Type == channelType {
			channels++
		}
	}

	if channels != 1 {
		t.Fatalf("API holder has %d *amqp.Channel fields, want exactly the publisher channel", channels)
	}
}

func TestProductionSourceNeverConsumes(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "production.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}

	ast.Inspect(file, func(node ast.Node) bool {
		sel, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		switch sel.Sel.Name {
		case "Consume", "PrepareWithPrefetch", "SetPrefetchCount", "Qos":
			t.Errorf("%s: API graph must not touch consumer plumbing (%s)", fset.Position(sel.Pos()), sel.Sel.Name)
		}

		return true
	})
}
