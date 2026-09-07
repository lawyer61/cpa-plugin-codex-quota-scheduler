package main

import (
	"errors"
	"strings"
	"sync"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

const completedRequestTombstoneLimit = 4096

var (
	ErrInflightCapacity   = errors.New("all eligible codex auths are at max inflight capacity")
	ErrRequestCorrelation = errors.New("request correlation is unavailable")
)

// InFlightSnapshot is a point-in-time view of logical request reservations.
type InFlightSnapshot struct {
	Counts         map[AuthInstanceID]int
	ActiveRequests int
	Total          int
}

type trackedLogicalRequest struct {
	sessionID    string
	reservations map[AuthInstanceID]string
}

// InFlightTracker owns logical request reservations and the plugin-private
// session-affinity cache. Reservations are released only by request.complete or
// an explicit rollback before an auth attempt starts.
type InFlightTracker struct {
	mu sync.Mutex

	requests       map[string]*trackedLogicalRequest
	counts         map[AuthInstanceID]int
	completed      map[string]struct{}
	completedOrder []string

	affinityEnabled bool
	affinityTTL     time.Duration
	affinity        *cliproxyauth.SessionCache
}

func NewInFlightTracker() *InFlightTracker {
	return &InFlightTracker{
		requests:  make(map[string]*trackedLogicalRequest),
		counts:    make(map[AuthInstanceID]int),
		completed: make(map[string]struct{}),
	}
}

// Begin records the host-generated logical request ID. Repeated calls are
// idempotent. A recently completed ID cannot be recreated by a late duplicate.
func (t *InFlightTracker) Begin(requestID, sessionID string) bool {
	if t == nil || strings.TrimSpace(requestID) == "" {
		return false
	}
	requestID = strings.TrimSpace(requestID)
	sessionID = strings.TrimSpace(sessionID)
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, completed := t.completed[requestID]; completed {
		return false
	}
	if existing := t.requests[requestID]; existing != nil {
		if existing.sessionID == "" && sessionID != "" {
			existing.sessionID = sessionID
		}
		return true
	}
	t.requests[requestID] = &trackedLogicalRequest{
		sessionID:    sessionID,
		reservations: make(map[AuthInstanceID]string),
	}
	return true
}

// TryAcquire atomically checks and reserves one slot. limit == 0 is unlimited.
// The same logical request reusing the same auth instance is idempotent.
func (t *InFlightTracker) TryAcquire(requestID string, instance AuthInstanceID, authID string, limit int) (allowed, added bool) {
	if t == nil || strings.TrimSpace(requestID) == "" || instance == 0 || strings.TrimSpace(authID) == "" || limit < 0 {
		return false, false
	}
	requestID = strings.TrimSpace(requestID)
	authID = strings.TrimSpace(authID)
	t.mu.Lock()
	defer t.mu.Unlock()
	request := t.requests[requestID]
	if request == nil {
		return false, false
	}
	if _, held := request.reservations[instance]; held {
		return true, false
	}
	if limit > 0 && t.counts[instance] >= limit {
		return false, false
	}
	request.reservations[instance] = authID
	t.counts[instance]++
	return true, true
}

// ReserveSynthetic reserves capacity for a plugin-owned request whose exact
// start and end are controlled by the plugin, such as a quota-window activation.
func (t *InFlightTracker) ReserveSynthetic(requestID string, instance AuthInstanceID, authID string, limit int) (func(), error) {
	if t == nil || !t.Begin(requestID, "") {
		return nil, ErrRequestCorrelation
	}
	allowed, _ := t.TryAcquire(requestID, instance, authID, limit)
	if !allowed {
		t.Complete(requestID)
		return nil, ErrInflightCapacity
	}
	var once sync.Once
	return func() { once.Do(func() { t.Complete(requestID) }) }, nil
}

// Rollback removes a reservation that was acquired but never reached an auth
// attempt, for example when a trial CAS loses after capacity admission.
func (t *InFlightTracker) Rollback(requestID string, instance AuthInstanceID) bool {
	if t == nil || strings.TrimSpace(requestID) == "" || instance == 0 {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	request := t.requests[strings.TrimSpace(requestID)]
	if request == nil {
		return false
	}
	if _, held := request.reservations[instance]; !held {
		return false
	}
	delete(request.reservations, instance)
	t.decrementLocked(instance)
	return true
}

// Complete releases every auth reservation held by one logical request.
func (t *InFlightTracker) Complete(requestID string) bool {
	if t == nil || strings.TrimSpace(requestID) == "" {
		return false
	}
	requestID = strings.TrimSpace(requestID)
	t.mu.Lock()
	defer t.mu.Unlock()
	request := t.requests[requestID]
	if request == nil {
		t.markCompletedLocked(requestID)
		return false
	}
	for instance := range request.reservations {
		t.decrementLocked(instance)
	}
	delete(t.requests, requestID)
	t.markCompletedLocked(requestID)
	return true
}

func (t *InFlightTracker) decrementLocked(instance AuthInstanceID) {
	if t.counts[instance] <= 1 {
		delete(t.counts, instance)
		return
	}
	t.counts[instance]--
}

func (t *InFlightTracker) markCompletedLocked(requestID string) {
	if _, exists := t.completed[requestID]; exists {
		return
	}
	t.completed[requestID] = struct{}{}
	t.completedOrder = append(t.completedOrder, requestID)
	if len(t.completedOrder) <= completedRequestTombstoneLimit {
		return
	}
	oldest := t.completedOrder[0]
	t.completedOrder = t.completedOrder[1:]
	delete(t.completed, oldest)
}

func (t *InFlightTracker) HasRequest(requestID string) bool {
	if t == nil || strings.TrimSpace(requestID) == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.requests[strings.TrimSpace(requestID)] != nil
}

func (t *InFlightTracker) HasReservation(requestID string, instance AuthInstanceID) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	request := t.requests[strings.TrimSpace(requestID)]
	if request == nil {
		return false
	}
	_, ok := request.reservations[instance]
	return ok
}

func (t *InFlightTracker) CanAcquire(requestID string, instance AuthInstanceID, limit int) bool {
	if t == nil || instance == 0 || limit < 0 {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	request := t.requests[strings.TrimSpace(requestID)]
	if request == nil {
		return false
	}
	if _, held := request.reservations[instance]; held {
		return true
	}
	return limit == 0 || t.counts[instance] < limit
}

func (t *InFlightTracker) Count(instance AuthInstanceID) int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.counts[instance]
}

func (t *InFlightTracker) Snapshot() InFlightSnapshot {
	out := InFlightSnapshot{Counts: make(map[AuthInstanceID]int)}
	if t == nil {
		return out
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out.ActiveRequests = len(t.requests)
	for instance, count := range t.counts {
		out.Counts[instance] = count
		out.Total += count
	}
	return out
}

// ConfigureAffinity replaces only the private affinity cache when its runtime
// configuration changes. In-flight reservations are deliberately untouched.
func (t *InFlightTracker) ConfigureAffinity(enabled bool, ttl time.Duration) {
	if t == nil {
		return
	}
	if ttl <= 0 {
		ttl = time.Hour
	}
	t.mu.Lock()
	if t.affinityEnabled == enabled && t.affinityTTL == ttl {
		t.mu.Unlock()
		return
	}
	old := t.affinity
	t.affinityEnabled = enabled
	t.affinityTTL = ttl
	t.affinity = nil
	if enabled {
		t.affinity = cliproxyauth.NewSessionCache(ttl)
	}
	t.mu.Unlock()
	if old != nil {
		old.Stop()
	}
}

func (t *InFlightTracker) AffinityKey(requestID, provider, model string) string {
	if t == nil {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.affinityEnabled {
		return ""
	}
	request := t.requests[strings.TrimSpace(requestID)]
	if request == nil || request.sessionID == "" {
		return ""
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		provider = "codex"
	}
	model = canonicalAffinityModel(model)
	return provider + "::" + request.sessionID + "::" + model
}

func canonicalAffinityModel(model string) string {
	model = strings.TrimSpace(model)
	if open := strings.LastIndex(model, "("); open >= 0 && strings.HasSuffix(model, ")") {
		if base := strings.TrimSpace(model[:open]); base != "" {
			model = base
		}
	}
	return strings.ToLower(model)
}

func (t *InFlightTracker) BoundAuth(key string) (string, bool) {
	if t == nil || key == "" {
		return "", false
	}
	t.mu.Lock()
	cache := t.affinity
	enabled := t.affinityEnabled
	t.mu.Unlock()
	if !enabled || cache == nil {
		return "", false
	}
	return cache.GetAndRefresh(key)
}

func (t *InFlightTracker) BindAuth(key, authID string) {
	if t == nil || key == "" || strings.TrimSpace(authID) == "" {
		return
	}
	t.mu.Lock()
	cache := t.affinity
	enabled := t.affinityEnabled
	t.mu.Unlock()
	if enabled && cache != nil {
		cache.Set(key, strings.TrimSpace(authID))
	}
}

// Stop stops the cache cleanup goroutine but intentionally does not release
// logical request reservations.
func (t *InFlightTracker) Stop() {
	if t == nil {
		return
	}
	t.mu.Lock()
	cache := t.affinity
	t.affinity = nil
	t.affinityEnabled = false
	t.mu.Unlock()
	if cache != nil {
		cache.Stop()
	}
}
