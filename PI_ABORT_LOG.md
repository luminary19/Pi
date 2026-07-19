# Lumicity Pi abort implementation log

This log records material implementation and distribution decisions for the durable cancellation work specified by `Kaizen/Pi/PI_ABORT.md` in the Lumicity workspace.

## Status

- Fork repository: `https://github.com/luminary19/Pi`
- Upstream repository: `https://github.com/earendil-works/pi`
- Working branch: `lumicity/abort-v0.80.10`
- Baseline: upstream tag `v0.80.10`, commit `8dc78834cde4e329284cf505f9e3f99763df5529`
- Live route during development: unchanged npm installation of `@earendil-works/pi-coding-agent@0.80.10`

## Decisions

### 2026-07-19 — Fork the complete upstream repository

The local repository is a full clone with upstream history and tags, not a reduced core extraction. `origin` points to `luminary19/Pi`; `upstream` points to `earendil-works/pi`.

Reason: cancellation crosses `packages/agent`, `packages/coding-agent`, tests, and release/update behavior. Keeping the complete monorepo makes upstream integration and whole-package verification possible.

### 2026-07-19 — Preserve upstream package identities

Keep the existing `@earendil-works/pi-*` package names, imports, dependencies, and `pi` binary identity.

Reason: Lumicity extensions resolve and import these public package identities. Renaming the package graph would create an avoidable compatibility boundary. The fork remains thin through a small, isolated commit stack rather than through renamed or partially copied packages.

### 2026-07-19 — Pin implementation work to the installed release baseline

Start the cancellation branch at the exact source commit published as the currently routed npm version `0.80.10`, then maintain it as a rebaseable Lumicity patch stack over upstream.

Reason: verification must compare like-for-like behavior before routing changes. Starting at current upstream `main` would combine cancellation changes with unrelated upstream evolution and weaken rollback confidence.

### 2026-07-19 — Do not change live Pi routing before verification

Do not modify the global npm installation, npm shim, PATH, Alacritty/zellij startup, or `PI_AGENT_CONFIG_DIR` routing during implementation. Build and test isolated artifacts first. Installation is a final updater-controlled operation only after source, packed-artifact, installed-runtime, and independent review gates pass.

Reason: the currently working npm installation is the rollback anchor and must remain untouched until the fork is proven.

### 2026-07-19 — Treat logical cancellation and physical cleanup separately

Implement prompt logical settlement as a core invariant with abort races, checkpoints, and stale-generation guards. Keep cooperative cleanup running in the background and expose it diagnostically without placing it on the editor-readiness critical path.

Reason: a non-cooperative provider, tool, extension listener, or Windows child process must not retain control of the interactive session after explicit abort.

### 2026-07-19 — Make physical work tracking promise-based, not timeout-based

Each run generation owns a dynamic physical-work tracker. When abort wins a race, the exact provider, iterator, tool, hook, update, or listener promise is registered with that tracker. `waitForLogicalIdle()` and the compatibility `waitForIdle()` resolve after coherent cancellation finalization; `waitForPhysicalSettlement()` resolves only after registered work actually settles. Detached rejections are retained in `physicalWorkFailures` with generation, label, error, and timestamp.

Reason: a fixed timeout can bound waiting but cannot establish ownership, suppress stale callbacks, or prove cleanup. Tracking the real promises preserves diagnostics without returning editor ownership to non-cooperative work.

### 2026-07-19 — Preserve transcript completeness on mid-batch abort

Every tool call in an already-committed assistant tool-use message receives one source-ordered terminal `ToolResultMessage`. Calls that emitted `tool_execution_start` also receive exactly one `tool_execution_end`; calls not yet started receive only a synthetic aborted result. Ordinary `afterToolCall` hooks are skipped when cancellation wins.

Reason: prompt cancellation must not leave malformed tool-call history that breaks the next provider request, while execution events must continue to describe work that actually started.

### 2026-07-19 — Core verification checkpoint

The isolated agent-core package builds successfully. New cancellation tests cover a never-settling tool, the parallel preparation race, a provider iterator that never yields, stale-generation suppression, physical settlement tracking, and a never-settling lifecycle listener. Existing `agent.test.ts` and `agent-loop.test.ts` suites remain green. The global npm route remains unchanged.

### 2026-07-19 — Keep session observers outside logical-idle ownership

`AgentSession` now treats extension/session event delivery as generation-owned physical work. Cancellation persists the coherent aborted transcript and emits settlement without waiting for a non-cooperative `message_end`/session observer; late callbacks are generation-gated and their failures remain observed.

Reason: extension observability must not be able to retain editor ownership or rewrite a later generation, but cancellation must still preserve the committed assistant/tool-result transcript before the next prompt.

### 2026-07-19 — Acknowledge Escape synchronously and measure logical latency

The interactive mode makes the first Escape acknowledgment immediate and idempotent, uses a stable `Cancelling...` to `Cancelled` state instead of an animation loop, and records bounded cancellation-latency samples for diagnostics.

Reason: cancellation feedback belongs to the input path, not to eventual provider/tool/process cleanup. Repeated Escape presses must not start parallel cancellation flows or hide the first request's timing.

### 2026-07-19 — Make Windows process cleanup observable without blocking readiness

Process-tree cleanup now returns an observable promise with `taskkill` exit/error/duration details and a direct-child fallback. Bash suppresses post-abort output immediately, requests termination once, and tracks bounded cleanup asynchronously after logical settlement.

Reason: Windows process teardown can be slow or partially fail, but that physical uncertainty must be diagnosable without freezing the editor or discarding the cleanup attempt.

### 2026-07-19 — Coding-agent verification checkpoint

The repository-wide `npm run check` gate passes. Targeted AgentSession persistence/settlement, interactive cancellation/status, and native Windows cleanup suites pass, and the coding-agent package builds. The live npm route remains unchanged.

### 2026-07-19 — Keep subagent ownership in the repo-owned extension

The separately maintained `@tintinweb/pi-subagents@0.14.1-lumicity.3` extension implements setup abort races, partial-session disposal, fresh spawn/resume controllers, prompt logical settlement, turn-owned direct background work, session-owned schedules, and a process-wide descendant cancellation graph. The extension is committed in the Lumicity repository at `a1704d6`.

Reason: this behavior belongs to the extension that creates and tracks descendants. Duplicating it into the Pi monorepo would create two authorities and make extension updates unsafe.

### 2026-07-19 — Disable canonical self-update in the fork

Every self-including built-in update form refuses before release lookup or package mutation and directs operators to `scripts/update-pi-lumicity.ps1`. Extension-only and model-catalog updates remain available.

Reason: the canonical updater can replace the fork with an official package of the same identity. A same-name fork must fail closed and route updates through the workflow that reapplies and verifies the Lumicity patch stack.

### 2026-07-19 — Package all forked identities as one verified artifact set

The release builder packs `@earendil-works/pi-ai`, `pi-tui`, `pi-agent-core`, and `pi-coding-agent` together, installs all four tarballs into an isolated global-prefix candidate, verifies exact names/versions, runs the candidate CLI, and executes a never-settling-tool abort probe against the installed files. SHA-256 hashes and source/upstream commits are written to `provenance.json`.

Reason: installing only the coding-agent tarball would allow npm to satisfy its core dependency from the canonical registry and silently lose the cancellation patch. The four-package artifact set preserves public identities while proving the installed dependency graph.

### 2026-07-19 — Keep live installation as an explicit final gate

`update-pi-lumicity.ps1` defaults to verification-only. `-Install` is required to change the global npm route, and post-install CLI plus abort probes must pass; otherwise the updater reinstalls the previous recorded artifact set or the previous canonical version.

Reason: source and tarball success are not sufficient evidence for changing the daily driver. Installation must remain explicit, reversible, and later than independent review.

## Planned commit units

1. Fork baseline and implementation record.
2. Agent-core bounded logical cancellation and regression tests.
3. Coding-agent UI/lifecycle and Windows cleanup hardening with tests.
4. Turn-owned subagent cancellation integration and tests.
5. Fail-closed updater, package/provenance tooling, and installed-runtime abort probe.
6. Verification and review remediations.
