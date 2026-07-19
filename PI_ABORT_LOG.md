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

## Planned commit units

1. Fork baseline and implementation record.
2. Agent-core bounded logical cancellation and regression tests.
3. Coding-agent UI/lifecycle and Windows cleanup hardening with tests.
4. Turn-owned subagent cancellation integration and tests.
5. Fail-closed updater, package/provenance tooling, and installed-runtime abort probe.
6. Verification and review remediations.
