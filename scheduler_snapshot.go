package main

import (
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

type SchedulerSnapshot struct {
	HandleEnabled              bool
	Fallback                   FallbackMode
	MonthlyMode                MonthlyMode
	MaxInflightRequestsPerAuth int
	SessionAffinityEnabled     bool
	Accounts                   []AccountView
	ActiveHighestTier          map[string]struct{}
	Trials                     *TrialRegistry
	InFlight                   *InFlightTracker
	EvidenceIntents            chan<- EvidenceIntent
	AdmissionVersion           uint64
	Activity                   func(pluginapi.SchedulerPickRequest, uint64, time.Time)
	Observation                func(pluginapi.SchedulerPickRequest, PickDecision, time.Time)
}

type EvidenceIntent struct {
	AuthID   string
	Instance AuthInstanceID
	BeganAt  time.Time
}

var publishedSchedulerSnapshot atomic.Pointer[SchedulerSnapshot]

func PublishSchedulerSnapshot(snapshot *SchedulerSnapshot) {
	if snapshot == nil {
		return
	}
	copy := cloneSchedulerSnapshot(*snapshot)
	publishedSchedulerSnapshot.Store(&copy)
}
func cloneSchedulerSnapshot(s SchedulerSnapshot) SchedulerSnapshot {
	s.Accounts = append([]AccountView(nil), s.Accounts...)
	s.ActiveHighestTier = cloneStringSet(s.ActiveHighestTier)
	return s
}
func cloneStringSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func schedulerPickPublished(req pluginapi.SchedulerPickRequest, now time.Time) PickDecision {
	snapshot := publishedSchedulerSnapshot.Load()
	if snapshot == nil {
		return PickDecision{Reason: "handle_disabled"}
	}
	if !requestIncludesCodex(req) {
		return PickDecision{Reason: "provider_not_codex"}
	}
	if !snapshot.HandleEnabled {
		return observeSchedulerDecision(snapshot, req, PickDecision{Reason: "handle_disabled"}, now)
	}
	if snapshot.Activity != nil {
		snapshot.Activity(req, snapshot.AdmissionVersion, now)
	}
	candidates := make([]Candidate, 0, len(req.Candidates))
	for _, candidate := range req.Candidates {
		candidates = append(candidates, Candidate{ID: candidate.ID, Provider: candidate.Provider})
	}
	if snapshot.MaxInflightRequestsPerAuth == 0 && !snapshot.SessionAffinityEnabled {
		return pickPublishedUntracked(snapshot, req, candidates, now)
	}
	if snapshot.InFlight == nil {
		return observeSchedulerDecision(snapshot, req, PickDecision{Handled: true, Reason: "request_correlation_unavailable", Err: ErrRequestCorrelation}, now)
	}
	requestID := schedulerRequestID(req.Options.Headers)
	if requestID == "" {
		return observeSchedulerDecision(snapshot, req, PickDecision{Handled: true, Reason: "request_correlation_missing", Err: ErrRequestCorrelation}, now)
	}
	affinityKey := snapshot.InFlight.AffinityKey(requestID, schedulerAffinityProvider(req), req.Model)
	skipped := make(map[AuthInstanceID]struct{})
	capacityBlocked := false
	if affinityKey != "" {
		if boundAuthID, ok := snapshot.InFlight.BoundAuth(affinityKey); ok {
			result := selectAccountByAuthID(*snapshot, candidates, boundAuthID, now, snapshot.Trials)
			if result.AuthID != "" {
				decision, decided, full := reservePublishedSelection(snapshot, req, result, requestID, affinityKey, now)
				if decided {
					return decision
				}
				capacityBlocked = full
				skipped[result.Instance] = struct{}{}
			}
		}
	}
	for {
		result := selectAccountSkipping(*snapshot, candidates, now, skipped, snapshot.Trials)
		if result.AuthID == "" {
			if capacityBlocked {
				return observeSchedulerDecision(snapshot, req, PickDecision{Handled: true, Reason: "inflight_capacity_full", Err: ErrInflightCapacity}, now)
			}
			return publishedFallbackDecision(snapshot, req, result, now)
		}
		decision, decided, full := reservePublishedSelection(snapshot, req, result, requestID, affinityKey, now)
		if decided {
			return decision
		}
		if full {
			capacityBlocked = true
		}
		skipped[result.Instance] = struct{}{}
	}
}

func pickPublishedUntracked(snapshot *SchedulerSnapshot, req pluginapi.SchedulerPickRequest, candidates []Candidate, now time.Time) PickDecision {
	result := selectAccountSkipping(*snapshot, candidates, now, nil, snapshot.Trials)
	var skipped map[AuthInstanceID]struct{}
	for result.AuthID != "" && result.Class == Opportunistic && (snapshot.Trials == nil || !snapshot.Trials.TryBegin(result.Instance, now)) {
		if skipped == nil {
			skipped = make(map[AuthInstanceID]struct{})
		}
		skipped[result.Instance] = struct{}{}
		result = selectAccountSkipping(*snapshot, candidates, now, skipped, snapshot.Trials)
	}
	if result.AuthID != "" && result.Class == Opportunistic {
		select {
		case snapshot.EvidenceIntents <- EvidenceIntent{AuthID: result.AuthID, Instance: result.Instance, BeganAt: now}:
			snapshot.Trials.MarkEvidencePending(result.Instance, true)
		default:
		}
	}
	if result.AuthID != "" {
		return observeSchedulerDecision(snapshot, req, PickDecision{AuthID: result.AuthID, Handled: true, Reason: "selected"}, now)
	}
	return publishedFallbackDecision(snapshot, req, result, now)
}

func reservePublishedSelection(snapshot *SchedulerSnapshot, req pluginapi.SchedulerPickRequest, result SelectionResult, requestID, affinityKey string, now time.Time) (PickDecision, bool, bool) {
	added, err := snapshot.InFlight.TryAcquire(requestID, result.Instance, result.AuthID, snapshot.MaxInflightRequestsPerAuth)
	if errors.Is(err, ErrInflightCapacity) {
		return PickDecision{}, false, true
	}
	if err != nil {
		reason := "request_correlation_unavailable"
		if errors.Is(err, ErrAuthInstanceUnavailable) {
			reason = "auth_instance_unavailable"
		} else if errors.Is(err, ErrInflightConfiguration) {
			reason = "inflight_configuration_invalid"
		}
		return observeSchedulerDecision(snapshot, req, PickDecision{Handled: true, Reason: reason, Err: err}, now), true, false
	}
	if result.Class == Opportunistic && added && (snapshot.Trials == nil || !snapshot.Trials.TryBegin(result.Instance, now)) {
		snapshot.InFlight.Rollback(requestID, result.Instance)
		return PickDecision{}, false, false
	}
	if result.Class == Opportunistic && added {
		select {
		case snapshot.EvidenceIntents <- EvidenceIntent{AuthID: result.AuthID, Instance: result.Instance, BeganAt: now}:
			snapshot.Trials.MarkEvidencePending(result.Instance, true)
		default:
		}
	}
	if affinityKey != "" {
		snapshot.InFlight.BindAuth(affinityKey, result.AuthID)
	}
	return observeSchedulerDecision(snapshot, req, PickDecision{AuthID: result.AuthID, Handled: true, Reason: "selected"}, now), true, false
}

func publishedFallbackDecision(snapshot *SchedulerSnapshot, req pluginapi.SchedulerPickRequest, result SelectionResult, now time.Time) PickDecision {
	if snapshot.Fallback == FallbackFillFirst {
		return observeSchedulerDecision(snapshot, req, PickDecision{Handled: true, DelegateBuiltin: pluginapi.SchedulerBuiltinFillFirst, Reason: result.Reason}, now)
	}
	return observeSchedulerDecision(snapshot, req, PickDecision{Reason: result.Reason}, now)
}

func schedulerRequestID(headers map[string][]string) string {
	for key, values := range headers {
		if !strings.EqualFold(key, inFlightRequestHeader) {
			continue
		}
		for _, value := range values {
			if value = strings.TrimSpace(value); value != "" {
				return value
			}
		}
	}
	return ""
}

func schedulerAffinityProvider(req pluginapi.SchedulerPickRequest) string {
	if provider := strings.ToLower(strings.TrimSpace(req.Provider)); provider != "" {
		return provider
	}
	return "codex"
}

func observeSchedulerDecision(snapshot *SchedulerSnapshot, req pluginapi.SchedulerPickRequest, decision PickDecision, now time.Time) PickDecision {
	if snapshot != nil && snapshot.Observation != nil {
		snapshot.Observation(req, decision, now)
	}
	return decision
}

func schedulerSnapshotFromState(state StateSnapshot, trials *TrialRegistry) *SchedulerSnapshot {
	active := cloneStringSet(state.CPAAdmission.AuthIDs)
	accounts := make([]AccountView, 0, len(state.Accounts))
	for _, a := range state.Accounts {
		accounts = append(accounts, accountViewFromState(a, state.Config, state.Now, trials))
	}
	var activity func(pluginapi.SchedulerPickRequest, uint64, time.Time)
	var observation func(pluginapi.SchedulerPickRequest, PickDecision, time.Time)
	if pump := globalPickActivityPump.Load(); pump != nil {
		activity = pump.enqueue
		observation = pump.enqueueObservation
	}
	return &SchedulerSnapshot{HandleEnabled: state.Config.HandleEnabled, Fallback: state.Config.Fallback, MonthlyMode: state.Config.MonthlyMode, MaxInflightRequestsPerAuth: state.Config.MaxInflightRequestsPerAuth, SessionAffinityEnabled: state.Config.SessionAffinityEnabled, Accounts: accounts, ActiveHighestTier: active, Trials: trials, InFlight: globalInFlightTracker, EvidenceIntents: globalEvidenceIntents, Activity: activity, Observation: observation}
}

func accountViewFromState(a AccountState, cfg Config, now time.Time, trials *TrialRegistry) AccountView {
	cache := CacheFresh
	if a.LastSuccessAt.IsZero() {
		cache = CacheUnknown
	} else if a.Stale {
		cache = CacheStale
	} else if now.Sub(a.LastSuccessAt) > cfg.QuotaRefreshInterval {
		cache = CacheAging
	}
	exhausted, reset := accountExhaustion(a, now)
	trial := TrialNone
	if trials != nil {
		trial = trials.State(a.Instance, now)
	}
	circuit := effectiveCircuitState(a.Circuit, now).EffectiveState
	circuitClass := CircuitClosed
	if circuit == CircuitStateOpen {
		circuitClass = CircuitOpen
	} else if circuit == CircuitStateHalfOpen {
		circuitClass = CircuitHalfOpen
	}
	return AccountView{
		ID: a.AuthID, AuthIndex: a.AuthIndex, Instance: a.Instance,
		PluginPriority: a.Annotation.SchedulerPriority, Family: a.Family,
		Cache: cache, LastKnownAvailable: a.LastError == "", Exhausted: exhausted,
		ResetAt: reset, AuthBlocked: a.Refresh.AuthFailure, Circuit: circuitClass,
		TemporaryUnavailable: a.TemporaryExhausted && a.TemporaryResetAt.After(now),
		Trial:                trial, Expiry: accountSortTime(a), RemainingQuota: remainingQuota(a), QuotaPressure: quotaPressure(a, now),
	}
}

func publishSchedulerState(state *PluginState, active map[string]struct{}, now time.Time) {
	if state == nil {
		return
	}
	s := state.Snapshot(now)
	if active != nil {
		s.CPAAdmission = CPAAdmissionState{Observed: true, AuthIDs: cloneStringSet(active)}
	}
	snapshot := schedulerSnapshotFromState(s, globalTrials)
	_, snapshot.AdmissionVersion = state.CPAAdmissionVersioned()
	PublishSchedulerSnapshot(snapshot)
}
func accountExhaustion(a AccountState, now time.Time) (bool, time.Time) {
	if windowExhausted(a.Quota.LongWindow, now) {
		return true, a.Quota.LongWindow.ResetAt
	}
	if windowExhausted(a.Quota.FiveHour, now) {
		return true, a.Quota.FiveHour.ResetAt
	}
	return false, time.Time{}
}
func remainingQuota(a AccountState) float64 {
	for _, w := range []*QuotaWindow{a.Quota.LongWindow, a.Quota.FiveHour} {
		if w != nil && w.UsedPercent != nil {
			return 100 - *w.UsedPercent
		}
	}
	return 0
}

const minimumQuotaPressureWindow = 30 * time.Minute

// quotaPressure estimates how quickly the remaining long-window quota must be
// consumed before reset. A 30-minute floor keeps the score bounded close to a
// reset. Unknown long-window usage has no pressure and falls through to the
// deterministic expiry/remaining-quota tie breakers.
func quotaPressure(a AccountState, now time.Time) float64 {
	window := a.Quota.LongWindow
	if window == nil || window.UsedPercent == nil || window.ResetAt.IsZero() {
		return 0
	}
	untilReset := window.ResetAt.Sub(now)
	if untilReset <= 0 {
		return 0
	}
	if untilReset < minimumQuotaPressureWindow {
		untilReset = minimumQuotaPressureWindow
	}
	remaining := 100 - *window.UsedPercent
	if remaining < 0 {
		remaining = 0
	} else if remaining > 100 {
		remaining = 100
	}
	return remaining / untilReset.Hours()
}
