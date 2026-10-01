// Package broker declares the job queues and publishes confirmed messages.
//
// [Declare] creates durable [QueueJobs], [domain.QueueDLQ], and the delay
// buckets. A delay queue expires through [ExchangeDelayDLX] back onto
// [QueueJobs]. [Publisher.Publish] sends a persistent message and waits for
// its confirm. A timeout is [ErrConfirmTimeout] and a nack is [ErrNack].
// [MarshalEnqueue] writes only a job id. [Ack] acknowledges one delivery.
// [NackDrop] does not requeue. Consumer prefetch defaults to [PrefetchCount].
// The package does not store the job.
package broker
