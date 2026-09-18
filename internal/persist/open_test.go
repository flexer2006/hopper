package persist //nolint:testpackage // unexported closer, adopt, Store fields

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type fakeClient struct {
	pingErr       error
	disconnectErr error
	pings         int
	disconnects   int
}

const replicaURI = "mongodb://localhost:27017/?replicaSet=rs0"

func (f *fakeClient) Ping(context.Context, *readpref.ReadPref) error {
	f.pings++

	return f.pingErr
}

func (f *fakeClient) Disconnect(context.Context) error {
	f.disconnects++

	return f.disconnectErr
}

func TestStoreOpenNilReceiver(t *testing.T) {
	t.Parallel()

	var store *Store

	err := store.Open(t.Context(), Options{URI: replicaURI})
	if !errors.Is(err, ErrNotOpen) {
		t.Fatalf("(*Store)(nil).Open() err = %v, want ErrNotOpen", err)
	}
}

func TestStoreOpenRejectsAlreadyOpenWithoutDialing(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	store := new(Store)
	store.client = client

	err := store.Open(t.Context(), Options{URI: replicaURI})
	if !errors.Is(err, ErrAlreadyOpen) {
		t.Fatalf("Open() on open store err = %v, want ErrAlreadyOpen", err)
	}

	if client.disconnects != 0 {
		t.Fatalf("Open() disconnected the live client %d times, want 0", client.disconnects)
	}

	if store.client != client {
		t.Fatal("Open() replaced the live client")
	}
}

func TestStoreAdoptCopiesEveryField(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	now := func() time.Time { return time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC) }
	fence := func() (string, error) { return "fence", nil }

	opened := new(Store)
	opened.coll = newMem(now)
	opened.now = now
	opened.newFence = fence
	opened.client = client
	opened.lease = 42 * time.Second

	store := new(Store)
	store.adopt(opened)

	if store.coll != opened.coll || store.client != client || store.lease != 42*time.Second {
		t.Fatalf("adopt() = %+v, want fields of %+v", store, opened)
	}

	if store.now == nil || !store.now().Equal(now()) {
		t.Fatal("adopt() did not carry now")
	}

	got, err := store.newFence()
	if err != nil || got != "fence" {
		t.Fatalf("adopt() newFence = %q %v", got, err)
	}
}

func TestStoreCloseReleasesClientAndPingReportsNotOpen(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	store := new(Store)
	store.coll = newMem(nil)
	store.client = client

	err := store.Ping(t.Context())
	if err != nil || client.pings != 1 {
		t.Fatalf("Ping() before Close err = %v pings = %d", err, client.pings)
	}

	err = store.Close(t.Context())
	if err != nil || client.disconnects != 1 {
		t.Fatalf("Close() err = %v disconnects = %d", err, client.disconnects)
	}

	err = store.Ping(t.Context())
	if !errors.Is(err, ErrNotOpen) {
		t.Fatalf("Ping() after Close err = %v, want ErrNotOpen", err)
	}

	if client.pings != 1 {
		t.Fatalf("Ping() after Close reached the driver (%d pings)", client.pings)
	}

	err = store.Close(t.Context())
	if err != nil || client.disconnects != 1 {
		t.Fatalf("second Close() err = %v disconnects = %d, want idempotent", err, client.disconnects)
	}

	if store.coll == nil {
		t.Fatal("Close() dropped coll; post-close calls must surface driver errors, not nil dereferences")
	}
}

func TestStoreCloseKeepsClientOnDisconnectError(t *testing.T) {
	t.Parallel()

	want := errors.New("disconnect")
	client := &fakeClient{disconnectErr: want}
	store := new(Store)
	store.client = client

	err := store.Close(t.Context())
	if !errors.Is(err, want) {
		t.Fatalf("Close() err = %v, want %v", err, want)
	}

	if store.client != client {
		t.Fatal("Close() released the client despite a failed Disconnect")
	}
}

func TestStorePingPropagatesDriverError(t *testing.T) {
	t.Parallel()

	want := errors.New("ping")
	store := new(Store)
	store.client = &fakeClient{pingErr: want}

	err := store.Ping(t.Context())
	if !errors.Is(err, want) {
		t.Fatalf("Ping() err = %v, want %v", err, want)
	}
}

func TestBindOpenCloseStartWrapsOpen(t *testing.T) {
	t.Parallel()

	store := new(Store)
	hooks := store.BindOpenClose(Options{}, 0)
	err := hooks.Start(t.Context())
	if !errors.Is(err, ErrStandalone) {
		t.Fatalf("start err = %v, want ErrStandalone", err)
	}

	err = hooks.Stop(t.Context())
	if err != nil {
		t.Fatalf("stop on unopened store = %v", err)
	}
}

func TestBindOpenCloseStopDisconnects(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	store := new(Store)
	store.client = client

	hooks := store.BindOpenClose(Options{}, time.Second)
	err := hooks.Stop(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if client.disconnects != 1 {
		t.Fatalf("disconnects = %d, want 1", client.disconnects)
	}
}
