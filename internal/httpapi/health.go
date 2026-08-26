package httpapi

import (
	"context"
	"net/http"
)

type Checker interface {
	Name() string
	Check(ctx context.Context) error
}

const (
	checkMongo    = "mongo"
	checkAMQP     = "amqp"
	checkUp       = "up"
	checkDown     = "down"
	statusOK      = "ok"
	statusUnavail = "unavailable"
)

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	checks := map[string]string{
		checkMongo: checkDown,
		checkAMQP:  checkDown,
	}

	byName := make(map[string]Checker, len(h.checks))
	for i := range h.checks {
		item := h.checks[i]
		if item == nil {
			continue
		}

		byName[item.Name()] = item
	}

	allUp := true

	for _, name := range []string{checkMongo, checkAMQP} {
		item, ok := byName[name]
		if !ok {
			allUp = false

			continue
		}

		err := item.Check(r.Context())
		if err != nil {
			allUp = false

			continue
		}

		checks[name] = checkUp
	}

	body := healthBody{Checks: checks, Status: statusUnavail}
	status := http.StatusServiceUnavailable

	if allUp {
		body.Status = statusOK
		status = http.StatusOK
	}

	writeJSON(w, status, body)
}
