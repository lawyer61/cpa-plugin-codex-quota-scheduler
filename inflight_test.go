package main

import (
	"sync"
	"testing"
	"time"
)

func TestInFlightTrackerReservationLifecycle(t *testing.T) {
	tracker := NewInFlightTracker()
	instanceA := AuthInstanceID(1)
	instanceB := AuthInstanceID(2)
	if !tracker.Begin("request-1", "session-1") {
		t.Fatal("Begin(request-1) = false")
	}
	if allowed, added := tracker.TryAcquire("request-1", instanceA, "auth-a", 1); !allowed || !added {
		t.Fatalf("first A acquire = %v,%v", allowed, added)
	}
	if allowed, added := tracker.TryAcquire("request-1", instanceA, "auth-a", 1); !allowed || added {
		t.Fatalf("duplicate A acquire = %v,%v", allowed, added)
	}
	if allowed, added := tracker.TryAcquire("request-1", instanceB, "auth-b", 1); !allowed || !added {
		t.Fatalf("B acquire = %v,%v", allowed, added)
	}
	if allowed, added := tracker.TryAcquire("request-1", instanceA, "auth-a", 1); !allowed || added {
		t.Fatalf("A retry acquire = %v,%v", allowed, added)
	}
	if got := tracker.Count(instanceA); got != 1 {
		t.Fatalf("A count = %d, want 1", got)
	}
	if got := tracker.Count(instanceB); got != 1 {
		t.Fatalf("B count = %d, want 1", got)
	}
	if !tracker.Complete("request-1") {
		t.Fatal("Complete(request-1) = false")
	}
	if tracker.Complete("request-1") {
		t.Fatal("duplicate Complete(request-1) = true")
	}
	if got := tracker.Snapshot(); got.Total != 0 || got.ActiveRequests != 0 {
		t.Fatalf("snapshot after complete = %#v", got)
	}
	if tracker.Begin("request-1", "new-session") {
		t.Fatal("recently completed request ID was recreated")
	}
}

func TestInFlightTrackerConcurrentCapacity(t *testing.T) {
	tracker := NewInFlightTracker()
	instance := AuthInstanceID(1)
	const requests = 32
	for i := 0; i < requests; i++ {
		tracker.Begin(string(rune('a'+i)), "")
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var winnersMu sync.Mutex
	winners := 0
	for i := 0; i < requests; i++ {
		requestID := string(rune('a' + i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			allowed, added := tracker.TryAcquire(requestID, instance, "auth-a", 1)
			if allowed && added {
				winnersMu.Lock()
				winners++
				winnersMu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	if winners != 1 {
		t.Fatalf("winners = %d, want 1", winners)
	}
	if got := tracker.Count(instance); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
}

func TestInFlightTrackerRollbackOnlyRemovesUnsentReservation(t *testing.T) {
	tracker := NewInFlightTracker()
	instance := AuthInstanceID(1)
	tracker.Begin("request-1", "")
	if allowed, added := tracker.TryAcquire("request-1", instance, "auth-a", 1); !allowed || !added {
		t.Fatalf("acquire = %v,%v", allowed, added)
	}
	if !tracker.Rollback("request-1", instance) {
		t.Fatal("Rollback = false")
	}
	if tracker.Rollback("request-1", instance) {
		t.Fatal("duplicate Rollback = true")
	}
	tracker.Begin("request-2", "")
	if allowed, _ := tracker.TryAcquire("request-2", instance, "auth-a", 1); !allowed {
		t.Fatal("capacity was not returned after rollback")
	}
}

func TestInFlightTrackerAffinityReconfigurePreservesReservations(t *testing.T) {
	tracker := NewInFlightTracker()
	defer tracker.Stop()
	instance := AuthInstanceID(1)
	tracker.ConfigureAffinity(true, time.Hour)
	tracker.Begin("request-1", "codex:session-1")
	key := tracker.AffinityKey("request-1", "codex", "gpt-5(high)")
	if key == "" {
		t.Fatal("AffinityKey = empty")
	}
	if lowKey := tracker.AffinityKey("request-1", "codex", "gpt-5(low)"); lowKey != key {
		t.Fatalf("thinking suffix changed affinity key: %q != %q", lowKey, key)
	}
	tracker.BindAuth(key, "auth-a")
	if got, ok := tracker.BoundAuth(key); !ok || got != "auth-a" {
		t.Fatalf("BoundAuth = %q,%v", got, ok)
	}
	if allowed, _ := tracker.TryAcquire("request-1", instance, "auth-a", 1); !allowed {
		t.Fatal("reservation acquire failed")
	}
	tracker.ConfigureAffinity(false, 2*time.Hour)
	if _, ok := tracker.BoundAuth(key); ok {
		t.Fatal("binding survived affinity disable")
	}
	if got := tracker.Count(instance); got != 1 {
		t.Fatalf("reservation count after affinity reconfigure = %d, want 1", got)
	}
	tracker.ConfigureAffinity(true, 2*time.Hour)
	if _, ok := tracker.BoundAuth(key); ok {
		t.Fatal("old binding survived cache replacement")
	}
}
