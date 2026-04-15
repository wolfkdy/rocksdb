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

**Status:** done. Implementation in `file/nonblocking_io_writable_file_writer.{h,cc}`; tests in `file/nonblocking_io_writable_file_writer_test.cc`. Per §4.4 Case 4, `slots_[0]` aliases `buf_` and `slots_[1]` aliases `flush_buf_` when `use_group_commit=false` — no separate scratch allocation. `BufferSlot` also carries `file_offset_start` (see §3.10).
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

**Status:** done. Implementation in `util/link_buf.{h,cc}`; `AdvanceWatermark()` returns the new watermark so callers can drive `VersionSet::SetLastSequenceAtLeast` (see §3.6 visibility-publish note below).
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

**Status:** done. Exposed as `log::SerializeRecord` + `log::ComputeSerializedRecordSize` (see `db/log_record_serializer.{h,cc}`). The paired allocator (1.4) calls `ComputeSerializedRecordSize` inside the spinlock so `byte_count` is derived from the same `abs_file_offset` that `SerializeRecord` later uses.
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

**Status:** done. `util/lsn_offset_allocator.{h,cc}`. Public API is `AdvanceForSerialize(lsn_count, payload_size, slot_file_offset_start, recycle_log_files, next_wal_offset)` → `{start_lsn, start_offset, byte_count}`; byte_count is computed under the spinlock to keep the paired advance torn-write-free.
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

**Status:** done. `db/preprocess_write_lockless.{h,cc}`. Slow-path reasons (§3.9.1 / §3.9.2) are enumerated in `PrewriteReason`; `WriteImplNoGroup` currently maps every non-ok reason to `Status::NotSupported` pending the 2.3/2.5 wiring.
**PR:** —

---

### 1.6 Feature flag `use_group_commit`

**Design ref:** §2.

**Scope:**
- Add `DBOptions::use_group_commit` (default `true` — i.e., leaderless path is opt-in via `use_group_commit=false`; this is the opposite polarity of the original sketch but reads better as "the traditional mode is the default").
- Add OpenDB-time validation that all §2 conditions hold when the flag is set to `false` (`nonblocking_wal_io=true`, `!two_write_queues`, `!unordered_write`, `!enable_pipelined_write`). `ColumnFamilyOptions::CheckConcurrentWritesSupported` is also required.
- Add per-write rejection for `WriteOptions::disableWAL=true`, `WriteOptions::sync=true`, and `WriteBatch::HasMerge()` when the flag is off.

**Tests:**
- Option parsing / persistence.
- OpenDB acceptance matrix.
- Per-write rejection matrix.

**Status:** done. Rejection matrix lives inside `WriteImplNoGroup` and the dispatch prologue at the top of `WriteImpl`.
**PR:** —

---

## 3. Round 2 — Integration with the core write path

Starts only after every Round 1 piece is merged. Pieces in this round are NOT independent — they touch `DBImpl::WriteImpl`, `SyncWAL`, `SwitchWAL`, `SwitchMemtable`, iterator construction, and related call sites. Each still lands as its own PR, but the dependency chain is linear.

### 2.1 New `WriteImplNoGroup` entry function

**Design ref:** §3.1 + §3.8.

**Scope:** new function gated by §1.6's flag. Calls §1.5 prewrite, §1.1 claim, §1.4 advance, memtable insert, §1.3 serializer + memcpy, §1.2 Link_buf store + advance, §1.1 release. Error on IO failure per §3.11.

**Status:** done. `db/db_impl/db_impl_write_no_group.cc`; dispatched from `DBImpl::WriteImpl` when `use_group_commit=false`. Tests in `db/db_write_test.cc :: DBWriteNoGroupCommitTest`.

Key implementation decisions beyond the design doc:
- Visibility is published via `VersionSet::SetLastSequenceAtLeast(new_watermark - 1)` — a CAS-max loop driven by `Link_buf::AdvanceWatermark()`'s return value. `SetLastSequence` would regress last_sequence_ under out-of-order writer completion.
- The log-writer snapshot at the top of the function holds `log_write_mutex_` and asserts `logs_.size() == 1` (Round-2 scope guard; piece 2.3's drain protocol obviates the mutex).
- Any post-claim failure (memtable insert, serialize size mismatch, slot-bytes overflow) aborts the process per §3.11 — the LSN space has advanced, and returning an error would leave a hole.
- `NonBlockingIOWritableFileWriter::AdvanceFileSize()` is called after every successful serialize so `GetFileSize()` stays honest for downstream observers (§3.9.1 WAL-too-big check, `FlushSwappedBuffer` invariant).
**PR:** —

---

### 2.2 New `SyncWAL` variant

**Design ref:** §3.4.

**Scope:** branches on the flag; new variant uses `CloseDrainOpen` + `PrepareForSync` / `FinishSync` / `log_sync_cv_` per §3.4.

**Tests:** concurrent-SyncWAL collapse, SyncWAL + writes interleaving, SyncWAL + structural-switch race.

**Status:** done (structural-switch-race test deferred until 2.3).
**PR:** —

Notes:
- Aliased `slot_bytes_[0/1]` onto `buf_`/`flush_buf_` per §4.4 Case 4 — one 128 MB/WAL allocation serves both roles; legacy mode (default) keeps the old small pre-alloc.
- Ctor now takes `use_group_commit` explicitly; DB-open threads `immutable_db_options_.use_group_commit` through.
- SyncWAL IO failure aborts the process (§3.11): Phase-3 has already published B, so returning an error would leave A's on-disk state undefined.
- Round-2 scope guard: `assert(logs_.size() == 1)` in both the snapshot and the not-switcher wait — lifted in 2.3.

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
| 1.1 Packed header + slot primitives | 1 | done | — |
| 1.2 Link_buf ring | 1 | done | — |
| 1.3 WAL serializer helper | 1 | done | — |
| 1.4 Paired LSN-offset allocator | 1 | done | — |
| 1.5 Lockless prewrite helper | 1 | done | — |
| 1.6 Feature flag (`use_group_commit`, default `true`) | 1 | done | — |
| 2.1 WriteImplNoGroup | 2 | done | — |
| 2.2 SyncWAL variant | 2 | done (structural-switch race test deferred to 2.3) | — |
| 2.3 Structural switch path | 2 | not started | — |
| 2.4 GetSnapshot / iterator wait | 2 | not started | — |
| 2.5 Stall path + tests | 2 | not started | — |
| 2.6 End-to-end + stress | 2 | blocked on 2.3, 2.4, 2.5 | — |
