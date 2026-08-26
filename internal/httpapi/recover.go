package httpapi

import (
	"errors"
	"net/http"

	"go.uber.org/zap"
)

func (h *Handler) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}

			err, ok := rec.(error)
			if ok && errors.Is(err, http.ErrAbortHandler) {
				panic(rec)
			}

			h.log.Error("http panic")
			writeErr(w, http.StatusInternalServerError, "internal error", codeServiceUnavailable)
		}()

		next.ServeHTTP(w, r)
	})
}

func zapJobID(id string) zap.Field {
	return zap.String("job_id", id)
}
