package fboot_test

import (
	"errors"
	"testing"
	"time"

	"github.com/flexer2006/hopper/internal/broker"
	"github.com/flexer2006/hopper/internal/fxx/fboot"
	"github.com/flexer2006/hopper/internal/persist"
	"github.com/flexer2006/hopper/internal/platform"
)

const (
	replicaMongoURI    = "mongodb://mongo:27017/?replicaSet=rs0"
	standaloneMongoURI = "mongodb://mongo:27017"
	validAMQPURI       = "amqp://rabbitmq:5672/"
)

func TestRequireInfrastructure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg  *platform.Config
		want error
		name string
	}{
		{
			name: "nil config",
			cfg:  nil,
			want: platform.ErrConfig,
		},
		{
			name: "missing",
			cfg:  new(platform.Config),
			want: platform.ErrConfig,
		},
		{
			name: "standalone",
			cfg:  &platform.Config{MongoURI: standaloneMongoURI, AMQPURI: validAMQPURI},
			want: persist.ErrStandalone,
		},
		{
			name: "bad amqp",
			cfg:  &platform.Config{MongoURI: replicaMongoURI, AMQPURI: "http://rabbitmq/"},
			want: broker.ErrURI,
		},
		{
			name: "bad exchange delay",
			cfg: &platform.Config{
				MongoURI:      replicaMongoURI,
				AMQPURI:       validAMQPURI,
				ExchangeDelay: "custom.dlx",
			},
			want: platform.ErrConfig,
		},
		{
			name: "lease budget exceeded",
			cfg: &platform.Config{
				MongoURI:    replicaMongoURI,
				AMQPURI:     validAMQPURI,
				ClaimLease:  time.Second,
				HTTPTimeout: 10 * time.Second,
			},
			want: persist.ErrLeaseBudget,
		},
		{
			name: "ok",
			cfg:  &platform.Config{MongoURI: replicaMongoURI, AMQPURI: validAMQPURI},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := fboot.RequireInfrastructure(tc.cfg)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("RequireInfrastructure() err = %v", err)
				}

				return
			}

			if !errors.Is(err, tc.want) {
				t.Fatalf("RequireInfrastructure() err = %v, want %v", err, tc.want)
			}
		})
	}
}
