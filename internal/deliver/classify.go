package deliver

import (
	"errors"
	"fmt"
	"net"

	"github.com/flexer2006/hopper/internal/domain"
)

func ClassifyLocal(err error) domain.FailureClass {
	if err == nil {
		return ""
	}

	switch {
	case errors.Is(err, ErrBlocked),
		errors.Is(err, ErrEmptyDNS),
		errors.Is(err, ErrInvalidHTTP),
		errors.Is(err, domain.ErrInvalidTarget),
		errors.Is(err, domain.ErrHTTPForbidden):
		return domain.ClassNonRetryableLocal
	case errors.Is(err, ErrBodyLimit):
		return domain.ClassRetryable
	}

	var dns *net.DNSError
	if errors.As(err, &dns) && dns.IsNotFound {
		return domain.ClassNonRetryableLocal
	}

	return domain.ClassRetryable
}

func ClassifyPost(res HTTPResult, postErr error) (domain.Outcome, domain.FailureClass, string) {
	if postErr != nil {
		class := ClassifyLocal(postErr)

		return domain.OutcomeFailure, class, postErr.Error()
	}

	outcome, class, err := domain.ClassifyHTTP(res.StatusCode)
	if err != nil {
		return domain.OutcomeFailure, domain.ClassRetryable, fmt.Sprintf("http %d", res.StatusCode)
	}

	if outcome == domain.OutcomeSuccess {
		return outcome, "", fmt.Sprintf("http %d", res.StatusCode)
	}

	return outcome, class, fmt.Sprintf("http %d", res.StatusCode)
}
