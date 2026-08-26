package persist

import (
	"context"
	"errors"
	"time"

	"github.com/flexer2006/hopper/internal/dispatch"
)

func clampScan(limit int) int {
	if limit <= 0 {
		return defaultScanLimit
	}

	if limit > maxScanLimit {
		return maxScanLimit
	}

	return limit
}

func mapIntents(docs []jobDoc) []dispatch.Intent {
	out := make([]dispatch.Intent, 0, len(docs))
	for i := range docs {
		out = append(out, docs[i].intent())
	}

	return out
}

func (s *Store) ListPending(ctx context.Context, limit int) ([]dispatch.Intent, error) {
	docs, err := s.coll.listPending(ctx, clampScan(limit))
	if err != nil {
		return nil, err
	}

	return mapIntents(docs), nil
}

func (s *Store) ListExpiredLeases(ctx context.Context, limit int) ([]string, error) {
	docs, err := s.coll.listExpiredLeases(ctx, clampScan(limit))
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(docs))
	for i := range docs {
		ids[i] = docs[i].ID
	}

	return ids, nil
}

func (s *Store) ListDueHealing(ctx context.Context, age time.Duration, limit int) ([]dispatch.Intent, error) {
	docs, err := s.coll.listDueHealing(ctx, age, clampScan(limit))
	if err != nil {
		return nil, err
	}

	return mapIntents(docs), nil
}

func (s *Store) PromoteDueRetry(ctx context.Context, id string, generation int) (dispatch.Intent, error) {
	doc, err := s.coll.promoteDueRetry(ctx, id, generation)

	return s.mapCAS(&doc, err)
}

func (s *Store) StartHealing(
	ctx context.Context,
	id string,
	generation int,
	age time.Duration,
) (dispatch.Intent, error) {
	doc, err := s.coll.startHealing(ctx, id, generation, age)

	return s.mapCAS(&doc, err)
}

func (s *Store) mapCAS(doc *jobDoc, err error) (dispatch.Intent, error) {
	if errors.Is(err, ErrNotFound) {
		return dispatch.Intent{}, dispatch.ErrNotFound
	}

	if err != nil {
		return dispatch.Intent{}, err
	}

	return doc.intent(), nil
}
