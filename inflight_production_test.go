package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func TestProductionRefreshFirstRequestWithInflightLimit(t *testing.T) {
	for _, affinity := range []bool{false, true} {
		t.Run(fmt.Sprintf("affinity=%v", affinity), func(t *testing.T) {
			testProductionInflightLimit(t, affinity)
		})
	}
}

func testProductionInflightLimit(t *testing.T, affinity bool) {
	now := time.Date(2026, 9, 7, 10, 20, 0, 0, time.UTC)
	cfg := DefaultConfig()
	cfg.HandleEnabled = true
	cfg.MaxInflightRequestsPerAuth = 4
	cfg.SessionAffinityEnabled = affinity
	tracker := NewInFlightTracker()
	tracker.ConfigureAffinity(affinity, time.Hour)
	state := NewPluginState(cfg)
	oldState, oldTracker, oldTrials := globalState, globalInFlightTracker, globalTrials
	oldSnapshot := publishedSchedulerSnapshot.Load()
	globalState, globalInFlightTracker, globalTrials = state, tracker, NewTrialRegistry()
	t.Cleanup(func() {
		tracker.Stop()
		globalState, globalInFlightTracker, globalTrials = oldState, oldTracker, oldTrials
		publishedSchedulerSnapshot.Store(oldSnapshot)
	})
	host := &countingProductionHost{
		httpResp: pluginapi.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"rate_limit":{"secondary_window":{"used_percent":20,"limit_window_seconds":604800,"reset_after_seconds":86400}}}`)},
		auth: map[string]pluginapi.HostAuthGetResponse{
			"a": {AuthIndex: "a", Name: "a.json", JSON: json.RawMessage(`{"access_token":"test-access","refresh_token":"test-refresh","account_id":"test-account"}`)},
		},
	}
	adapter := &rosterCredentialHost{host: host, roster: HostRosterSnapshot{Capability: CapabilityB}}
	refresher, err := NewProductionQuotaRefresher(host, state, adapter, HostRosterSnapshot{Capability: CapabilityB}, filepath.Join(t.TempDir(), "state.json"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(refresher.Stop)
	adapter.bindings = refresher.bindings
	roster := HostRosterSnapshot{Capability: CapabilityA, Confirmed: true, Health: RosterHealthy, BackgroundAllowed: true, Entries: []RosterEntry{
		{ID: "a", AuthIndex: "a", Provider: "codex", Priority: intPtr(9)},
	}}
	if err := refresher.PublishAuthoritativeRoster(context.Background(), roster); err != nil {
		t.Fatal(err)
	}
	refresher.RefreshOnce()
	if got := state.Snapshot(now).Accounts; len(got) != 1 || got[0].LastSuccessAt.IsZero() {
		t.Fatal("production quota refresh did not populate a healthy auth")
	}
	if tracker.Snapshot().Total != 0 {
		t.Fatal("fixture is not idle")
	}

	before := interceptRequestBefore(tracker, cfg, pluginapi.RequestInterceptRequest{
		RequestID: "first-request", Headers: http.Header{"Session-Id": {"shared-session"}}, Body: []byte(`{"model":"gpt-6-astra"}`),
	})
	if before.Terminate {
		t.Fatal("BeforeAuth rejected the first request")
	}
	raw, err := json.Marshal(pluginapi.SchedulerPickRequest{
		Providers:  []string{"codex"},
		Model:      "gpt-6-astra",
		Options:    pluginapi.SchedulerOptions{Headers: before.Headers},
		Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "a", Provider: "codex", Priority: 9}},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := handleSchedulerPick(raw)
	if err != nil {
		binding, _ := refresher.bindings.Lookup("a")
		t.Fatalf("idle first request rejected: %v; occupied=%d; binding_instance=%d; account_instance=%d", err, tracker.Snapshot().Total, binding.Instance, state.Snapshot(now).Accounts[0].Instance)
	}
	var env envelope
	if err := json.Unmarshal(encoded, &env); err != nil {
		t.Fatal(err)
	}
	var selected pluginapi.SchedulerPickResponse
	if err := json.Unmarshal(env.Result, &selected); err != nil {
		t.Fatal(err)
	}
	if selected.AuthID != "a" || !selected.Handled {
		t.Fatalf("first request did not select healthy auth: %#v", selected)
	}
	if tracker.Snapshot().Total != 1 {
		t.Fatal("first request did not acquire exactly one slot")
	}
	// A repeated pick is still the same logical request, not a new reservation.
	if _, err := handleSchedulerPick(raw); err != nil {
		t.Fatal(err)
	}
	if tracker.Snapshot().Total != 1 {
		t.Fatal("repeat pick duplicated the reservation")
	}

	// A refresh must preserve the identity used by already-running requests.
	refresher.RefreshOnce()
	binding, _ := refresher.bindings.Lookup("a")
	if state.Snapshot(now).Accounts[0].Instance != binding.Instance || tracker.Count(binding.Instance) != 1 {
		t.Fatal("quota refresh changed the identity of an occupied auth")
	}
	pickNew := func(id string) (pluginapi.SchedulerPickResponse, error) {
		t.Helper()
		before := interceptRequestBefore(tracker, cfg, pluginapi.RequestInterceptRequest{
			RequestID: id, Headers: http.Header{"Session-Id": {"shared-session"}}, Body: []byte(`{"model":"gpt-6-astra"}`),
		})
		if before.Terminate {
			t.Fatal("BeforeAuth unexpectedly terminated")
		}
		candidates := []pluginapi.SchedulerAuthCandidate{}
		for _, entry := range roster.Entries {
			candidates = append(candidates, pluginapi.SchedulerAuthCandidate{ID: entry.ID, Provider: "codex", Priority: 9})
		}
		raw, err := json.Marshal(pluginapi.SchedulerPickRequest{Providers: []string{"codex"}, Model: "gpt-6-astra", Options: pluginapi.SchedulerOptions{Headers: before.Headers}, Candidates: candidates})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := handleSchedulerPick(raw)
		if err != nil {
			return pluginapi.SchedulerPickResponse{}, err
		}
		var env envelope
		if err := json.Unmarshal(encoded, &env); err != nil {
			t.Fatal(err)
		}
		var response pluginapi.SchedulerPickResponse
		if err := json.Unmarshal(env.Result, &response); err != nil {
			t.Fatal(err)
		}
		return response, nil
	}
	for i := 2; i <= 4; i++ {
		if got, err := pickNew(fmt.Sprintf("request-%d", i)); err != nil || got.AuthID != "a" {
			t.Fatalf("request %d should still fit on a: %#v, %v", i, got, err)
		}
	}
	if _, err := pickNew("overflow"); !errors.Is(err, ErrInflightCapacity) {
		t.Fatalf("fifth live request must be refused: %v", err)
	}
	if tracker.Count(binding.Instance) != 4 {
		t.Fatal("auth a did not retain exactly four reservations")
	}

	// Add a second healthy production auth. The full affinity binding must move.
	host.auth["b"] = pluginapi.HostAuthGetResponse{AuthIndex: "b", Name: "b.json", JSON: json.RawMessage(`{"access_token":"test-b","refresh_token":"test-rb","account_id":"test-account-b"}`)}
	roster.Entries = append(roster.Entries, RosterEntry{ID: "b", AuthIndex: "b", Provider: "codex", Priority: intPtr(9)})
	if err := refresher.PublishAuthoritativeRoster(context.Background(), roster); err != nil {
		t.Fatal(err)
	}
	refresher.RefreshOnce()
	if got, err := pickNew("failover"); err != nil || got.AuthID != "b" {
		t.Fatalf("full affinity auth should switch to production auth b: %#v, %v", got, err)
	}
	bindingB, _ := refresher.bindings.Lookup("b")
	if tracker.Count(binding.Instance) != 4 || tracker.Count(bindingB.Instance) != 1 {
		t.Fatal("failover did not use independent auth capacity")
	}

	for _, id := range []string{"first-request", "request-2", "request-3", "request-4", "overflow", "failover"} {
		raw, _ := json.Marshal(pluginapi.RequestCompletion{RequestID: id, Outcome: pluginapi.RequestCompletionSucceeded})
		if _, err := handleRequestComplete(raw); err != nil {
			t.Fatal(err)
		}
		if _, err := handleRequestComplete(raw); err != nil {
			t.Fatal(err)
		}
	}
	if got := tracker.Snapshot(); got.Total != 0 || got.ActiveRequests != 0 {
		t.Fatal("completion left request reservations behind")
	}
	wantAfterComplete := "a"
	if affinity {
		wantAfterComplete = "b"
	}
	if got, err := pickNew("after-complete"); err != nil || got.AuthID != wantAfterComplete {
		t.Fatalf("updated affinity was not reused after completion: %#v, %v", got, err)
	}
	tracker.Complete("after-complete")

	// A pre-fix cached account must not erase the newly supplied identity on merge.
	old := state.Snapshot(now).Accounts[0]
	old.Instance = 0
	state.UpsertQuota(old)
	refresher.RefreshOnce()
	for _, account := range state.Snapshot(now).Accounts {
		binding, _ := refresher.bindings.Lookup(account.AuthID)
		if account.Instance == 0 || account.Instance != binding.Instance {
			t.Fatal("refresh retained a zero or mismatched instance")
		}
	}

}

func TestSchedulerInvalidStateIsNotCapacityExhaustion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		instance AuthInstanceID
		begin    bool
		complete bool
	}{
		{name: "missing auth instance", begin: true},
		{name: "unknown request", instance: 1},
		{name: "completed request", instance: 1, begin: true, complete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewInFlightTracker()
			t.Cleanup(tracker.Stop)
			previous := publishedSchedulerSnapshot.Load()
			t.Cleanup(func() { publishedSchedulerSnapshot.Store(previous) })
			if tc.begin {
				tracker.Begin("request", "session")
			}
			if tc.complete {
				tracker.Complete("request")
			}
			PublishSchedulerSnapshot(&SchedulerSnapshot{
				HandleEnabled: true, MaxInflightRequestsPerAuth: 4,
				Accounts:          []AccountView{{ID: "a", Instance: tc.instance, Cache: CacheFresh}},
				ActiveHighestTier: map[string]struct{}{"a": {}},
				Trials:            NewTrialRegistry(), InFlight: tracker,
			})
			got := schedulerPickPublished(pluginapi.SchedulerPickRequest{
				Provider: "codex", Model: "gpt-6-astra",
				Options:    pluginapi.SchedulerOptions{Headers: map[string][]string{inFlightRequestHeader: {"request"}}},
				Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "a", Provider: "codex"}},
			}, time.Now())
			wantErr, wantReason := ErrRequestCorrelation, "request_correlation_unavailable"
			if tc.instance == 0 {
				wantErr, wantReason = ErrAuthInstanceUnavailable, "auth_instance_unavailable"
			}
			if !errors.Is(got.Err, wantErr) || got.Reason != wantReason || got.DelegateBuiltin != "" || got.AuthID != "" {
				t.Fatalf("invalid state must be rejected without calling it capacity exhaustion: %#v", got)
			}
			if got := tracker.Snapshot().Total; got != 0 {
				t.Fatalf("invalid state acquired %d slots", got)
			}
		})
	}
}
