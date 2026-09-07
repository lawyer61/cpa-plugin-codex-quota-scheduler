package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func TestInflightConfigAndRegistration(t *testing.T) {
	cfg, err := DecodeConfig([]byte("max_inflight_requests_per_auth: 2\nsession_affinity_enabled: true\nsession_affinity_ttl: 45m\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxInflightRequestsPerAuth != 2 || !cfg.SessionAffinityEnabled || cfg.SessionAffinityTTL != 45*time.Minute {
		t.Fatalf("decoded config = %#v", cfg)
	}
	if _, err := DecodeConfig([]byte("max_inflight_requests_per_auth: -1\n")); err == nil {
		t.Fatal("negative max_inflight_requests_per_auth was accepted")
	}
	if _, err := DecodeConfig([]byte("session_affinity_ttl: 0s\n")); err == nil {
		t.Fatal("non-positive session_affinity_ttl was accepted")
	}
	defaults := DefaultConfig()
	if defaults.MaxInflightRequestsPerAuth != 0 || defaults.SessionAffinityEnabled || defaults.SessionAffinityTTL != time.Hour {
		t.Fatalf("defaults = %#v", defaults)
	}
	payload := SettingsFromConfig(cfg)
	roundTrip, err := ConfigFromSettings(DefaultConfig(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if roundTrip.MaxInflightRequestsPerAuth != 2 || !roundTrip.SessionAffinityEnabled || roundTrip.SessionAffinityTTL != 45*time.Minute {
		t.Fatalf("settings round trip = %#v", roundTrip)
	}
	payload.MaxInflightRequestsPerAuth = -1
	if _, err := ConfigFromSettings(DefaultConfig(), payload); err == nil {
		t.Fatal("settings accepted a negative in-flight limit")
	}
	reg := PluginRegistration()
	if !reg.Capabilities.RequestInterceptor || !reg.Capabilities.RequestLifecyclePlugin {
		t.Fatalf("lifecycle capabilities missing: %#v", reg.Capabilities)
	}
}

func TestRequestLifecycleCorrelationOverridesSpoofAndClearsBeforeUpstream(t *testing.T) {
	tracker := NewInFlightTracker()
	defer tracker.Stop()
	tracker.ConfigureAffinity(true, time.Hour)
	cfg := DefaultConfig()
	cfg.MaxInflightRequestsPerAuth = 1
	cfg.SessionAffinityEnabled = true

	before := interceptRequestBefore(tracker, cfg, pluginapi.RequestInterceptRequest{
		RequestID: "host-request-1",
		Headers:   http.Header{inFlightRequestHeader: []string{"client-spoof"}, "Session-Id": []string{"thread-1"}},
		Body:      []byte(`{"model":"gpt-5-codex"}`),
	})
	if before.Terminate {
		t.Fatalf("before terminated: %#v", before)
	}
	if got := before.Headers.Get(inFlightRequestHeader); got != "host-request-1" {
		t.Fatalf("authoritative marker = %q", got)
	}
	if len(before.ClearHeaders) != 1 || before.ClearHeaders[0] != inFlightRequestHeader {
		t.Fatalf("clear headers = %#v", before.ClearHeaders)
	}
	if key := tracker.AffinityKey("host-request-1", "codex", "gpt-5-codex"); key == "" {
		t.Fatal("canonical session identity was not recorded")
	}

	after := interceptRequestAfter(tracker, pluginapi.RequestInterceptRequest{
		RequestID: "host-request-1",
		Headers:   http.Header{inFlightRequestHeader: []string{"host-request-1"}},
	})
	if after.Terminate || len(after.ClearHeaders) != 1 || after.ClearHeaders[0] != inFlightRequestHeader {
		t.Fatalf("valid after-auth response = %#v", after)
	}
	invalid := interceptRequestAfter(tracker, pluginapi.RequestInterceptRequest{RequestID: "host-request-1", Headers: http.Header{}})
	if !invalid.Terminate || invalid.StatusCode != http.StatusInternalServerError {
		t.Fatalf("invalid marker was not rejected: %#v", invalid)
	}
	tracker.Complete("host-request-1")
}

func TestSchedulerAffinitySwitchesWhenBoundAuthIsFullAndRejectsWhenAllFull(t *testing.T) {
	tracker := NewInFlightTracker()
	defer tracker.Stop()
	tracker.ConfigureAffinity(true, time.Hour)
	previous := publishedSchedulerSnapshot.Load()
	t.Cleanup(func() { publishedSchedulerSnapshot.Store(previous) })

	PublishSchedulerSnapshot(&SchedulerSnapshot{
		HandleEnabled:              true,
		Fallback:                   FallbackFillFirst,
		MonthlyMode:                MonthlyModeExpiryOrder,
		MaxInflightRequestsPerAuth: 1,
		SessionAffinityEnabled:     true,
		Accounts: []AccountView{
			{ID: "auth-a", Instance: 1, Cache: CacheFresh, PluginPriority: 20},
			{ID: "auth-b", Instance: 2, Cache: CacheFresh, PluginPriority: 10},
		},
		ActiveHighestTier: map[string]struct{}{"auth-a": {}, "auth-b": {}},
		Trials:            NewTrialRegistry(),
		InFlight:          tracker,
	})
	request := func(id string) pluginapi.SchedulerPickRequest {
		return pluginapi.SchedulerPickRequest{
			Provider: "codex",
			Model:    "gpt-5-codex",
			Options:  pluginapi.SchedulerOptions{Headers: map[string][]string{inFlightRequestHeader: {id}}},
			Candidates: []pluginapi.SchedulerAuthCandidate{
				{ID: "auth-a", Provider: "codex", Priority: 7},
				{ID: "auth-b", Provider: "codex", Priority: 7},
			},
		}
	}

	tracker.Begin("request-1", "codex:shared-thread")
	if got := schedulerPickPublished(request("request-1"), time.Now()); got.AuthID != "auth-a" || got.Err != nil {
		t.Fatalf("first pick = %#v", got)
	}
	tracker.Begin("request-2", "codex:shared-thread")
	if got := schedulerPickPublished(request("request-2"), time.Now()); got.AuthID != "auth-b" || got.Err != nil {
		t.Fatalf("full affinity failover pick = %#v", got)
	}
	if tracker.Count(1) != 1 || tracker.Count(2) != 1 {
		t.Fatalf("counts = %#v", tracker.Snapshot())
	}

	tracker.Begin("request-3", "codex:shared-thread")
	raw, _ := json.Marshal(request("request-3"))
	if _, err := handleSchedulerPick(raw); !errors.Is(err, ErrInflightCapacity) {
		t.Fatalf("all-full scheduler error = %v", err)
	}
	if tracker.Count(1) != 1 || tracker.Count(2) != 1 {
		t.Fatalf("all-full path changed counts: %#v", tracker.Snapshot())
	}

	tracker.Complete("request-1")
	tracker.Complete("request-2")
	tracker.Complete("request-3")
	tracker.Begin("request-4", "codex:shared-thread")
	if got := schedulerPickPublished(request("request-4"), time.Now()); got.AuthID != "auth-b" || got.Err != nil {
		t.Fatalf("updated affinity binding was not reused: %#v", got)
	}
	tracker.Complete("request-4")
}

func TestSchedulerLimitRequiresLifecycleCorrelation(t *testing.T) {
	previous := publishedSchedulerSnapshot.Load()
	t.Cleanup(func() { publishedSchedulerSnapshot.Store(previous) })
	PublishSchedulerSnapshot(&SchedulerSnapshot{
		HandleEnabled:              true,
		MaxInflightRequestsPerAuth: 1,
		Accounts:                   []AccountView{{ID: "auth-a", Instance: 1, Cache: CacheFresh}},
		ActiveHighestTier:          map[string]struct{}{"auth-a": {}},
		Trials:                     NewTrialRegistry(),
		InFlight:                   NewInFlightTracker(),
	})
	got := schedulerPickPublished(pluginapi.SchedulerPickRequest{
		Provider:   "codex",
		Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "auth-a", Provider: "codex"}},
	}, time.Now())
	if !errors.Is(got.Err, ErrRequestCorrelation) || got.DelegateBuiltin != "" {
		t.Fatalf("missing correlation decision = %#v", got)
	}
}

func TestSyntheticReservationSharesAuthCapacity(t *testing.T) {
	tracker := NewInFlightTracker()
	release, err := tracker.ReserveSynthetic("probe-1", 1, "auth-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tracker.ReserveSynthetic("probe-2", 1, "auth-a", 1); !errors.Is(err, ErrInflightCapacity) {
		t.Fatalf("second synthetic reservation error = %v", err)
	}
	release()
	release()
	secondRelease, err := tracker.ReserveSynthetic("probe-3", 1, "auth-a", 1)
	if err != nil {
		t.Fatalf("capacity was not returned: %v", err)
	}
	secondRelease()
}

func TestRequestCompleteABIDispatchReleasesEveryTerminalOutcome(t *testing.T) {
	previous := globalInFlightTracker
	tracker := NewInFlightTracker()
	globalInFlightTracker = tracker
	t.Cleanup(func() { globalInFlightTracker = previous })
	outcomes := []pluginapi.RequestCompletionOutcome{
		pluginapi.RequestCompletionSucceeded,
		pluginapi.RequestCompletionFailed,
		pluginapi.RequestCompletionRejected,
		pluginapi.RequestCompletionCanceled,
	}
	for i, outcome := range outcomes {
		requestID := "complete-" + string(rune('a'+i))
		tracker.Begin(requestID, "")
		if _, err := tracker.TryAcquire(requestID, 1, "auth-a", 10); err != nil {
			t.Fatalf("acquire %s failed", requestID)
		}
		raw, _ := json.Marshal(pluginapi.RequestCompletion{RequestID: requestID, Outcome: outcome})
		if _, err := handleMethod(pluginabi.MethodRequestComplete, raw); err != nil {
			t.Fatalf("request.complete %s: %v", outcome, err)
		}
	}
	if got := tracker.Snapshot(); got.Total != 0 || got.ActiveRequests != 0 {
		t.Fatalf("completion dispatch left reservations: %#v", got)
	}
}

func TestManagementStatusShowsInflightCapacity(t *testing.T) {
	previous := globalInFlightTracker
	tracker := NewInFlightTracker()
	globalInFlightTracker = tracker
	t.Cleanup(func() { globalInFlightTracker = previous })
	tracker.Begin("status-request", "")
	if _, err := tracker.TryAcquire("status-request", 1, "auth-a", 1); err != nil {
		t.Fatal("status reservation failed")
	}
	cfg := DefaultConfig()
	cfg.MaxInflightRequestsPerAuth = 1
	now := time.Now()
	account := weeklyAccount("auth-a", 5, now.Add(time.Hour), false)
	account.Instance = 1
	snapshot := StateSnapshot{Config: cfg, Accounts: []AccountState{account}, Now: now}
	payload := BuildStatusPayload(snapshot, []ScheduledAccount{{AuthID: "auth-a", Available: true}})
	if len(payload.Accounts) != 1 || payload.Accounts[0].InflightRequests != 1 || !payload.Accounts[0].CapacityFull {
		t.Fatalf("status account = %#v", payload.Accounts)
	}
	html := string(RenderStatusHTML(payload))
	for _, marker := range []string{"maxInflight", "sessionAffinityEnabled", "sessionAffinityTTL", "并发占位 1 / 1"} {
		if !strings.Contains(html, marker) {
			t.Fatalf("management HTML missing %q", marker)
		}
	}
	tracker.Complete("status-request")
}

func TestRepublishSchedulerConfigAppliesLimitWithoutDroppingActiveTier(t *testing.T) {
	previous := publishedSchedulerSnapshot.Load()
	t.Cleanup(func() { publishedSchedulerSnapshot.Store(previous) })
	PublishSchedulerSnapshot(&SchedulerSnapshot{HandleEnabled: true, ActiveHighestTier: map[string]struct{}{"auth-a": {}}})
	cfg := DefaultConfig()
	cfg.MaxInflightRequestsPerAuth = 3
	cfg.SessionAffinityEnabled = true
	store := NewPluginState(cfg)
	republishSchedulerConfig(store, time.Now())
	got := publishedSchedulerSnapshot.Load()
	if got == nil || got.MaxInflightRequestsPerAuth != 3 || !got.SessionAffinityEnabled {
		t.Fatalf("republished config = %#v", got)
	}
	if _, ok := got.ActiveHighestTier["auth-a"]; !ok {
		t.Fatalf("active tier was dropped: %#v", got.ActiveHighestTier)
	}
}
