package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

const bearerScheme = "bearer"

func (h *Handler) bearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vals := r.Header.Values("Authorization")
		if len(vals) != 1 {
			writeErr(w, http.StatusUnauthorized, "unauthorized", codeUnauthorized)

			return
		}

		got, ok := parseBearer(vals[0])
		if !ok || !tokenEqual(h.token, got) {
			writeErr(w, http.StatusUnauthorized, "unauthorized", codeUnauthorized)

			return
		}

		next.ServeHTTP(w, r)
	})
}

func parseBearer(header string) (string, bool) {
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, bearerScheme) || token == "" {
		return "", false
	}

	return token, true
}

func tokenEqual(want []byte, got string) bool {
	if len(want) == 0 || got == "" {
		return false
	}

	sumWant := sha256.Sum256(want)
	sumGot := sha256.Sum256([]byte(got))

	return subtle.ConstantTimeCompare(sumWant[:], sumGot[:]) == 1
}
