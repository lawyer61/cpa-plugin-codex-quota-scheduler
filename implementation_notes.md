# Implementation Notes

## Design decisions
- Fixed the host SDK baseline at CLIProxyAPI v7.2.152. The plugin registers `request.intercept_before`, `request.intercept_after`, and `request.complete`; no CPA host source is modified.
- The limit counts conservative logical-request reservations per auth instance. One request may reserve A and B across retries, and releases all held reservations only at logical completion.
- Session affinity is plugin-private, disabled by default, and configured independently from CPA routing affinity. It reuses v7.2.152 session identity and cache behavior, but commits a binding only after capacity admission succeeds.
- A full bound auth may switch to the next eligible auth inside `scheduler.pick`. When every eligible auth is full, the scheduler returns an error instead of delegating to an unlimited built-in selector.
- State remains in process memory. No database, Redis, distributed lock, persistent lease, executor rewrite, or host patch was added.
- PRs #4 through #10 were merged before feature work. Conflict resolution preserves the opt-in management-key storage, quota-pressure ordering, reset-window activation, reset countdowns, localization, English sidebar name, and quota-bar coloring.

## Modules
- `inflight.go`: logical request ledger, atomic per-auth admission, bounded completion tombstones, synthetic probe reservations, and plugin-private affinity cache.
- `dispatch.go`: v7.2.152 request interceptor/lifecycle ABI handlers and protected RequestID header bridge.
- `scheduler_snapshot.go` / `selection.go`: affinity-aware selection, full-auth skipping, atomic reservation, conservative retry behavior, and all-full rejection.
- `config.go` / `management.go`: configuration, Management API/UI, import/export, live snapshot republishing, and per-auth in-flight status.
- `probe_runtime.go`: shares per-auth capacity with the plugin's reset-window activation HTTP request and postpones activation when full.
- `docs/deviations.md`: records the bounded in-memory `Pick` side-effect as DEV-015.
- `README.md` / `README.zh-CN.md`: documents configuration, semantics, CPA v7.2.152 requirement, and the all-full HTTP behavior.

## How to run
```bash
export PATH=/opt/go1.26.0/bin:$PATH
make test
go test -race ./...
go vet ./...
make build
go run ./scripts/refactor_gates/analyze_pick_path.go -root . -entry handleSchedulerPick
```

## Implemented
- Integrated every open upstream PR frozen on 2026-09-07: #4, #5, #6, #7, #8, #9, and #10.
- Added `max_inflight_requests_per_auth` (`0` means unlimited).
- Added independent `session_affinity_enabled` and `session_affinity_ttl` settings; affinity defaults to off.
- Added spoof-resistant RequestID correlation from BeforeAuth to scheduler Pick and removal before upstream execution.
- Added idempotent per-request/per-auth reservations, multi-auth conservative retry accounting, and release for succeeded, failed, rejected, and canceled completions.
- Added bound-auth failover, all-full rejection without builtin fallback, UI status, settings persistence/import/export, and live reconfiguration without clearing existing reservations.
- Added capacity admission for reset-window activation requests.

## Not implemented / known limitations
- Reservations are local to one active plugin instance; there is no cross-process or distributed limit.
- `request.complete` is asynchronous. A lost callback can leave a conservative reservation until the plugin instance is replaced; no TTL-based guess or forced reset is used.
- Counts are logical reservations, not exact physical HTTP attempt or socket counts.
- CPA v7.2.152 exposes no scheduler response with a custom capacity status. A scheduler error reliably blocks fallback, and the host maps the plain error to HTTP 500.
- Plugin affinity is not shared with CPA's built-in affinity cache or global routing settings. Home/direct paths that bypass the verified interceptor/scheduler lifecycle are outside the guarantee.
- Enabling the feature while requests are already past BeforeAuth cannot reconstruct those earlier in-flight requests; subsequent correlated requests are limited normally.

## Observed results
- `make test`: passed.
- `go test -race ./...`: passed.
- `go vet ./...`: passed.
- `make build`: passed; generated the Linux amd64 shared library in `dist/`.
- Pick-path analyzer returned `[]`, matching the empty ratchet baseline.
- CPA v7.2.152 official contract tests passed for scheduler-error propagation, request-lifecycle RPC capability, AfterAuth termination/header clearing, unique request IDs, and plain-error HTTP 500 mapping.
- Every frozen PR head (#4-#10) is an ancestor of the integration branch.

## Other things that user need to note
- Development and validation were performed in the private CNB workspace branch `codex/integrate-open-prs-and-inflight-limit`.
- The optional browser management-key persistence from PR #4 remains disabled by default and stores plaintext only when the user explicitly enables it on a trusted browser.
- No release or tag was created.
