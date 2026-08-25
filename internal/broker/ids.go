package broker

import "time"

const (
	QueueJobs             = "jobs"
	ExchangeDelayDLX      = "jobs.delay.dlx"
	RoutingKeyJobs        = QueueJobs
	PrefetchCount         = 1
	DefaultConfirmTimeout = 5 * time.Second

	hexIDLen             = 24
	hexHashLen           = 64
	msPerSecond          = 1000
	classicDelayCount    = 7
	delayBucketProbe     = 8
	prefetchSizeBytes    = 0
	workAndDLQCount      = 2
	publishMandatory     = true
	publishImmediate     = false
	defaultExchange      = ""
	ackMultiple          = false
	nackRequeue          = false
	argDLX               = "x-dead-letter-exchange"
	argDLRK              = "x-dead-letter-routing-key"
	contentTypeJSON      = "application/json"
	exchangeKindDirect   = "direct"
	reasonMissing        = "missing_document"
	reasonMalformed      = "malformed_message"
	reasonAttempts       = "attempts_exhausted"
	reasonTerminalHTTP   = "terminal_http"
	reasonNonRetryable   = "non_retryable_local"
	reasonOperatorManual = "operator_manual"
)

func validHex(value string, length int) bool {
	if len(value) != length {
		return false
	}

	for i := range length {
		char := value[i]
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}

	return true
}

func knownDLQReason(reason string) bool {
	switch reason {
	case reasonAttempts, reasonTerminalHTTP, reasonNonRetryable, reasonOperatorManual:
		return true
	default:
		return false
	}
}
