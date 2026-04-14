# Eliminate Group Commit — Implementation Plan

**Companion to:** `eliminate_group_commit_design.md`. This doc tracks the "how and in what order"; the design doc tracks the "what and why". Each piece below cross-references the specific design section it implements.

**Last updated:** 2026-04-14.

---

## 1. Overview

The implementation is split into two rounds:

- **Round 1 — independent building blocks.** Each piece is a pure addition: new code, new tests, no existing-path call sites touched. Pieces are mutually independent and can be built in parallel or any order. Each is small enough to land as a single PR.
- **Round 2 — integration with the core write path.** Only starts after Round 1 is complete. This is where `DBImpl::WriteImpl`, `SyncWAL`, `SwitchWAL`, and related call sites are modified.

Rationale for the split: Round 1 lets us build and unit-test every primitive in isolation, under TSAN, against the design doc's invariants — before any master-mode behavior is at risk. By the time Round 2 starts, the primitives are proven and the integration work is mostly wiring.

**Master-mode protection.** Every piece in Round 1 must leave existing `nonblocking_wal_io` and default-mode behavior bit-identical. The feature flag added in §1.6 is the only way any of the new code paths can run; with the flag off, existing tests must continue to pass unchanged.

---

## 2. Round 1 — Independent building blocks

### 1.1 Packed header + slot primitives on `NonBlockingIOWritableFileWriter`

**Design ref:** §3.5 (layout, claim, release, close-drain-open).

**Scope:**
- Add `std::atomic<uint64_t> header_` per buffer, bits 0..47 = `in_flight_count`, bits 48..49 = `state`, bits 50..63 reserved. See §3.5.1.
- Add `next_wal_offset_` per buffer.
- Add methods: `ClaimSlot() -> Buffer*`, `ReleaseSlot(Buffer*)`, `CloseDrainOpen(Buffer* A, Buffer* B) -> {SWITCHER, NOT_SWITCHER}`.
- Default-mode preservation: `header_` initialized to `{state=ACTIVE, count=0}`; existing `SwapBuffer()` / `FlushSwappedBuffer()` code paths must not read or write the new fields. This piece is invisible to any caller that doesn't invoke the new methods.

**Tests:**
- CAS race: N claimers vs. one closer. Correct final state (`PENDING` → drained → `FREE`).
- Drain correctness: switcher's phase-2 wait does not complete until every `ReleaseSlot` fires.
- State-transition invariants: no writer sees `state != ACTIVE` with a successful claim.
- Stress under TSAN.

**Status:** not started.
**PR:** —

---

### 1.2 `Link_buf` circular ring

**Design ref:** §3.6.

**Scope:**
- Standalone class, likely in `util/link_buf.h` + `.cc`.
- API: `Store(start_lsn, end_lsn)`, `AdvanceWatermark()`, `min_non_hole_lsn()` load.
- `kCapacity = 64K` compile-time constant (see §3.6.1).
- Oversize batch rejection (§3.6.4 check 1).
- Ring-full abort (§3.6.4 check 2).

**Tests:**
- Hole tolerance: gap between two batches blocks watermark advance until the gap fills.
- Watermark walk: concurrent stores + opportunistic advances converge.
- Concurrent stores + advances: no lost updates, watermark is monotone.
- Oversize batch returns `Status::Busy`.
- Ring-full triggers abort (death test).

**Status:** not started.
**PR:** —

---

### 1.3 Per-writer WAL serializer helper

**Design ref:** §3.10 + §4.4 Case 2.

**Scope:**
- Free function: `SerializeWriteBatchForWAL(const WriteBatch&, uint64_t abs_file_offset, char* dst) -> size_t`.
- Output must be wire-format-identical to `log::Writer::AddRecord`, including the 7-byte header, CRC, and 32KB-block fragmentation (FIRST/MIDDLE/LAST splits). The `abs_file_offset` argument is required for correct block-boundary splits.

**Tests:**
- Round-trip via `log::Reader` for various batch sizes (1 byte, 31KB, 32KB, 33KB, 100KB, 1MB).
- Batches that cross 1 / 2 / many block boundaries.
- Torn-tail: truncate output at various points and verify `log::Reader` stops cleanly.
- Byte-for-byte comparison against `log::Writer::AddRecord` output for the same batch.

**Status:** not started.
**PR:** —

---

### 1.4 Paired LSN-offset allocator

**Design ref:** §3.2 (paired advance under spinlock).

**Scope:**
- Small class wrapping a spinlock plus references to `next_lsn` and one buffer's `next_wal_offset`.
- API: `Advance(lsn_count, byte_count) -> {start_lsn, start_offset}`.
- Spinlock body is four plain stores and two plain loads — no branches (§3.2.2).

**Tests:**
- Single-thread monotonicity.
- N-writer contention: partial-order invariant (§3.2.3) holds — ordering of `start_lsn` matches ordering of `start_offset` for any pair of writers.
- No torn writes: reading `(next_lsn, next_wal_offset)` mid-advance never observes an inconsistent pair.

**Status:** not started.
**PR:** —

---

### 1.5 Lockless prewrite helper

**Design ref:** §3.8 + §4.5.

**Scope:**
- Free function: `PreprocessWriteLockless(DBImpl*, const WriteOptions&, const WriteBatch*) -> PrewriteResult`.
- Reads the same condition sources the existing `DBImpl::PreprocessWrite` (db/db_impl/db_impl_write.cc:1114) reads, but with no locks. All of the existing condition tests (DB stopped, WAL too big, WBM should flush, trim_history, flush_scheduler, stall, WBM stall, need_log_sync) are already atomic / concurrent-safe reads (see §3.8 table).
- Includes batch validation: `VerifyChecksum()` + format check (§4.5).
- Return value encodes the outcome: `ok`, one of the structural-switch reasons (§3.9.1 triggers), one of the stall reasons (§3.9.2 triggers), or a validation failure.

**Tests:**
- Each condition source, flipped on in isolation, produces the corresponding slow-path reason.
- No condition set → returns `ok`.
- Corrupt batch → returns validation failure.

**Status:** not started.
**PR:** —

---

### 1.6 Feature flag `eliminate_group_commit`

**Design ref:** §2.

**Scope:**
- Add `DBOptions::eliminate_group_commit` (default `false`).
- Add OpenDB-time validation that all §2 conditions hold when the flag is on (`nonblocking_wal_io=true`, `!two_write_queues`, `!unordered_write`, `!enable_pipelined_write`).
- Add per-write rejection for `WriteOptions::disableWAL=true` and `WriteOptions::sync=true` when the flag is on.
- No behavior change — the flag is not yet read by the write path.

**Tests:**
- Option parsing / persistence.
- OpenDB acceptance matrix: flag on + all conditions met → accept; flag on + any condition violated → reject with a clear error.
- Per-write rejection matrix.

**Status:** not started.
**PR:** —

---

## 3. Round 2 — Integration with the core write path

Starts only after every Round 1 piece is merged. Pieces in this round are NOT independent — they touch `DBImpl::WriteImpl`, `SyncWAL`, `SwitchWAL`, `SwitchMemtable`, iterator construction, and related call sites. Each still lands as its own PR, but the dependency chain is linear.

### 2.1 New `WriteImplNoGroup` entry function

**Design ref:** §3.1 + §3.8.

**Scope:** new function gated by §1.6's flag. Calls §1.5 prewrite, §1.1 claim, §1.4 advance, memtable insert, §1.3 serializer + memcpy, §1.2 Link_buf store + advance, §1.1 release. Error on IO failure per §3.11.

**Status:** not started.
**PR:** —

---

### 2.2 New `SyncWAL` variant

**Design ref:** §3.4.

**Scope:** branches on the flag; new variant uses `CloseDrainOpen` + `PrepareForSync` / `FinishSync` / `log_sync_cv_` per §3.4.

**Tests:** concurrent-SyncWAL collapse, SyncWAL + writes interleaving, SyncWAL + structural-switch race.

**Status:** not started.
**PR:** —

---

### 2.3 Structural switch path

**Design ref:** §3.9.1.

**Scope:** implements the full §3.9.1 sequence: `CloseDrainOpen` → `mutex_` → `SwitchWAL` / `SwitchMemtable` / `TrimMemtableHistory` / `ScheduleFlushes` → publish B → schedule async flush of A via `PrepareForSync → ... → FinishSync + SignalAll`.

**Tests:** each triggering condition, coalescing of multiple conditions, async-flush-of-A vs. concurrent SwitchMemtable race (covered by existing `IsSyncing()` plumbing).

**Status:** not started.
**PR:** —

---

### 2.4 GetSnapshot / iterator `min_non_hole_lsn` wait

**Design ref:** §3.7.

**Scope:** `GetSnapshot` pins `snapshot_lsn = next_lsn - 1` without waiting. First read under the snapshot waits on `min_non_hole_lsn ≥ snapshot_lsn`.

**Tests:** read-your-own-write, snapshot consistency under concurrent writes, wait-bound measurement.

**Status:** not started.
**PR:** —

---

### 2.5 Stall path + §4.8 test suite

**Design ref:** §3.9.2 + §4.8.

**Scope:** slow-path serializer split per §3.9.2. The six TSAN tests enumerated in §4.8.

**Status:** not started.
**PR:** —

---

### 2.6 End-to-end, stress, crash-recovery

**Scope:**
- `db_stress` under the new flag.
- Crash/recovery tests for torn-tail cases.
- Performance comparison vs. the existing `nonblocking_wal_io` path.

**Status:** not started.
**PR:** —

---

## 4. Cross-cutting concerns

- **TSAN coverage.** Every piece in Round 1 that touches shared state (1.1, 1.2, 1.4) must have stress tests that run under TSAN. Round 2 integration tests must run under TSAN before merge.
- **Master-mode regression protection.** Every PR in Round 1 must pass the existing test suite unchanged. Round 2 PRs must also pass existing tests, and must not regress default-mode or `nonblocking_wal_io` performance.
- **Rollout strategy.** Flag defaults to `false`. Internal workloads opt in first. Monitor crash reports (process-abort on IO error per §3.11 is the primary new failure mode). Only flip the default after soak time.
- **v2 deferred work (§4.3, §4.9).** Not in scope for this plan. Once v1 lands and runs, revisit the soft-cap / overflow-handling design.

---

## 5. Status dashboard

| Piece | Round | Status | PR |
|---|---|---|---|
| 1.1 Packed header + slot primitives | 1 | not started | — |
| 1.2 Link_buf ring | 1 | not started | — |
| 1.3 WAL serializer helper | 1 | not started | — |
| 1.4 Paired LSN-offset allocator | 1 | not started | — |
| 1.5 Lockless prewrite helper | 1 | not started | — |
| 1.6 Feature flag | 1 | not started | — |
| 2.1 WriteImplNoGroup | 2 | blocked on R1 | — |
| 2.2 SyncWAL variant | 2 | blocked on 2.1 | — |
| 2.3 Structural switch path | 2 | blocked on 2.2 | — |
| 2.4 GetSnapshot / iterator wait | 2 | blocked on 2.1 | — |
| 2.5 Stall path + tests | 2 | blocked on 2.1 | — |
| 2.6 End-to-end + stress | 2 | blocked on 2.3, 2.4, 2.5 | — |
