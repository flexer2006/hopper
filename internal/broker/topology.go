package broker

import (
	"math"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/flexer2006/hopper/internal/domain"
)

func delayBuckets() ([]int, error) {
	out := make([]int, 0, classicDelayCount)
	seen := make(map[int]struct{}, classicDelayCount)

	for attempts := range delayBucketProbe {
		seconds, err := domain.BackoffSeconds(attempts)
		if err != nil {
			return nil, err
		}

		if _, dup := seen[seconds]; dup {
			if len(out) != classicDelayCount {
				return nil, ErrDelayBuckets
			}

			return out, nil
		}

		seen[seconds] = struct{}{}
		out = append(out, seconds)
	}

	if len(out) != classicDelayCount {
		return nil, ErrDelayBuckets
	}

	return out, nil
}

func DelayQueues() ([]string, error) {
	buckets, err := delayBuckets()
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(buckets))

	for _, seconds := range buckets {
		name, delayErr := domain.DelayQueue(seconds)
		if delayErr != nil {
			return nil, delayErr
		}

		names = append(names, name)
	}

	return names, nil
}

func TTLMillis(seconds int) int32 {
	if seconds < 0 || seconds > math.MaxInt32/msPerSecond {
		return 0
	}

	return int32(seconds) * int32(msPerSecond)
}

func DelayQueueArgs(seconds int) amqp.Table {
	return amqp.Table{
		amqp.QueueMessageTTLArg: TTLMillis(seconds),
		argDLX:                  ExchangeDelayDLX,
		argDLRK:                 RoutingKeyJobs,
	}
}

func AllQueueNames() ([]string, error) {
	delay, err := DelayQueues()
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, workAndDLQCount+len(delay))
	out = append(out, QueueJobs, domain.QueueDLQ)
	out = append(out, delay...)

	return out, nil
}
