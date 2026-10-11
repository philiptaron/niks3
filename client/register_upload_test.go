package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
)

func TestRegisterUploadedObject_BoundedAgainstSilentServer(t *testing.T) {
	t.Parallel()

	var (
		received atomic.Int32
		stop     = make(chan struct{})
	)

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		received.Add(1)

		select {
		case <-stop:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(stop)

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	const bound = time.Second

	c.SetRegistrationTimeout(bound)

	pushCtx, cancelPush := context.WithCancel(t.Context())
	start := time.Now()

	const registrations = 3
	for range registrations {
		c.RegisterUploadedObject(pushCtx, "abc.narinfo")
	}

	for received.Load() < registrations {
		if time.Since(start) > 10*time.Second {
			t.Fatalf("server saw %d registrations, want %d", received.Load(), registrations)
		}

		time.Sleep(time.Millisecond)
	}

	cancelPush()

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.WaitRegistrations()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("WaitRegistrations did not return: registrations against a silent server are unbounded")
	}

	if elapsed := time.Since(start); elapsed < bound {
		t.Errorf("registrations ended after %v, before their bound: cancelled with the push", elapsed)
	}
}
