package domain_test

import (
	"testing"

	"github.com/flexer2006/hopper/internal/domain"
)

func BenchmarkAdmitTargetHostname(b *testing.B) {
	const raw = "https://example.invalid/hop16"
	b.ReportAllocs()
	for b.Loop() {
		_, err := domain.AdmitTarget(raw)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClassifyHTTP200(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, _, err := domain.ClassifyHTTP(200)
		if err != nil {
			b.Fatal(err)
		}
	}
}
