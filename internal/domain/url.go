package domain

import (
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"
)

const (
	MaxTargetRunes = 2048
	schemeHTTP     = "http"
	schemeHTTPS    = "https"
)

func ParseTarget(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, fmt.Errorf("%w: empty", ErrInvalidTarget)
	}

	if utf8.RuneCountInString(raw) > MaxTargetRunes {
		return nil, fmt.Errorf("%w: length", ErrInvalidTarget)
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidTarget, err)
	}

	if !parsed.IsAbs() {
		return nil, fmt.Errorf("%w: not absolute", ErrInvalidTarget)
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != schemeHTTP && scheme != schemeHTTPS {
		return nil, fmt.Errorf("%w: scheme", ErrInvalidTarget)
	}

	if parsed.User != nil {
		return nil, fmt.Errorf("%w: userinfo", ErrInvalidTarget)
	}

	if parsed.Opaque != "" {
		return nil, fmt.Errorf("%w: opaque", ErrInvalidTarget)
	}

	if parsed.Host == "" {
		return nil, fmt.Errorf("%w: host", ErrInvalidTarget)
	}

	return parsed, nil
}

func ValidateTarget(raw string) error {
	_, err := ParseTarget(raw)

	return err
}
