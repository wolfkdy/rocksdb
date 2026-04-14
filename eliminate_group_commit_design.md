# Eliminate Group Commit — Design Notes

**Status:** Design discussion in progress. Last updated 2026-04-10.
**Branch context:** Builds on `work-from-7.7.fb`, on top of the `nonblocking_wal_io` feature (NonBlockingIOWritableFileWriter with ping-pong buffers, commits `3e20b442f`..`3efbf64da`).
**Document purpose:** Capture every decision made so far, plus all open items, in enough detail that a fresh session can resume the discussion without re-deriving context.

---

## 1. Goal

Eliminate RocksDB's WriteThread leader/follower group-commit machinery on the write path. Each writer thread runs prewrite, allocates its own LSN and WAL byte range, writes its own memtable entries, writes its own WAL bytes into a shared buffer, and returns — with **no leader**, **no follower wait**, and **no write-group context switches**.

This is a **new write path**, gated by configuration. It does not extend or modify `unordered_write` (which still has write groups).

---

## 2. Mode gating (configuration constraints)

The new path is active only when **all** of the following hold:

| Option | Required value | Reason |
|---|---|---|
| `nonblocking_wal_io` | `true` | Buffer infrastructure depends on it |
| `two_write_queues` | `false` | Single-queue assumption throughout |
| `unordered_write` | `false` | unordered_write has its own (leader-based) path |
| `enable_pipelined_write` | `false` | Pipelined write has its own state machine |
| `WriteOptions::disableWAL` | `false` (rejected per write) | Independent-writer model assumes a WAL slot per write |
| `WriteOptions::sync` | `false` (rejected per write) | sync=true would force per-write buffer swap |
| 2PC writes (Prepare/Commit separation) | not allowed | Depend on leader's atomic seq+WAL publish |

This new path is for `manual_wal_flush=false` (nonblocking_wal_io's domain). `manual_wal_flush=true` retains its existing behavior and is out of scope.

---

## 3. Confirmed design

### 3.1 Independent writer pipeline

Each writer thread executes the following sequence with no leader and no group:

```
WriteImpl_NoGroup(batch):
  1. Run per-thread prewrite (lockless checks; see §3.8)
  2. If any prewrite check fires → enter slow-path serializer (§3.9.1 or §3.9.2)
  3. Claim in-flight slot in the current active buffer via packed-header CAS (§3.5)
     - Load active_buffer_ptr = A; CAS A.header to increment count iff state == ACTIVE
     - On CAS failure: retry the loop (re-read active_buffer_ptr and A.header)
     - If state is persistently PENDING, a switch is in progress and the writer
       spins until the switcher publishes the new active buffer B (§3.5.4)
  4. Spinlock-protected paired advance of (next_lsn, A.next_wal_offset) (§3.2)
  5. Write batch entries to memtable (concurrent SkipList insert)
  6. memcpy serialized WAL bytes into A.bytes[] at the reserved offset
  7. Store completion link and advance min_non_hole_lsn watermark (§3.6)
  8. Atomic fetch_sub on A.header — decrements count, releases the slot (§3.5)
  9. Return success
```

**After step 3 the write is committed.** There is no error path, no rollback, no abort. Anything that could fail or wait must happen in step 1 (prewrite). Any IO error in steps 5–8 triggers process abort (§3.11).

WriteThread coordination is fully bypassed — no `WriteThread::JoinBatchGroup`, no leader/follower roles, no `LinkOne` linked list.

### 3.2 Paired (next_lsn, next_wal_offset) advance

By the time a writer reaches this step, it already **owns an in-flight slot** in a specific active buffer `A`, obtained via the packed-header CAS described in §3.5. This section only covers the second half of the claim: advancing the lsn and the buffer-local write offset as a pair so that the partial-order invariant holds within `A`.

#### 3.2.1 Fields

- `next_lsn`: global `uint64_t`. Shared across all buffers. Monotonic for the lifetime of the DB. Advanced by writers under `lsn_offset_spinlock`. Not reset across buffer switches.
- `A.next_wal_offset`: per-buffer `uint64_t`. Each buffer has its own. Reset to `0` when the buffer transitions FREE→ACTIVE during a switch (§3.5.4 phase 3). Represents the next write position inside `A`'s in-memory byte buffer (`A.bytes[]`).
- `lsn_offset_spinlock`: one global spinlock that serializes pair-advance across all writers. Not used by the switcher; not used by SyncWAL.

#### 3.2.2 Advance protocol (inside the spinlock)

```
spinlock_acquire(lsn_offset_spinlock)
  lsn              = next_lsn
  offset           = A.next_wal_offset
  next_lsn        += my_batch_lsn_count
  A.next_wal_offset += my_serialized_size
spinlock_release(lsn_offset_spinlock)
```

Note carefully what this spinlock body does **not** contain:

- **No state check.** Switch state is encoded in `A.header` (§3.5) and was verified atomically as part of the CAS in §3.5. Once the writer holds an in-flight slot in `A`, the switcher (§3.5.4, §3.9.1) is guaranteed to wait for this writer's `fetch_sub` before it begins flushing `A` — so `A.bytes[]` and `A.next_wal_offset` are stable for the duration of this spinlock body, regardless of any switch attempt the switcher may have already begun.
- **No in-flight count update.** That is done by the packed-header CAS in §3.5, strictly before this spinlock is acquired.
- **No active-buffer-pointer load.** `A` is captured by value from the §3.5 step; it does not change under the writer.

The spinlock body is four plain stores and two plain loads. No conditionals. No branches. No atomic operations beyond the spinlock primitives themselves.

#### 3.2.3 Partial-order invariant (within buffer `A`)

> For any two writers i and j that both claim in the same buffer A:
> `lsn[i] < lsn[j]  ⇔  A_offset[i] < A_offset[j]`

Holds because `lsn_offset_spinlock` serializes the pair-advance — both fields are advanced together inside one critical section, so their relative order is identical for any pair of writers in the same buffer.

#### 3.2.4 Cross-buffer lsn monotonicity

Writers in the old buffer `A` all finish their lsn advance **before** the switcher opens a new buffer `B`. This is enforced by the close-drain-open sequence in §3.5.4:

1. Switcher closes `A` (CAS state ACTIVE→PENDING).
2. Switcher waits for `A.in_flight_count == 0`. The wait covers every writer that ever held a slot in `A`, including writers that were past their §3.5 CAS but had not yet acquired `lsn_offset_spinlock`.
3. Only after the drain completes does the switcher publish `B` as the new active buffer.

Therefore: at the moment `B` first accepts a writer's CAS, every advance of `next_lsn` by an `A`-writer has already happened. All lsns in `A` are strictly less than all lsns in `B`. The WAL file, when flushed in buffer order (`A` then `B`), is monotonic in lsn across the buffer boundary. Recovery (§4.4) does not need any special handling for buffer transitions.

#### 3.2.5 Implementation choice: spinlock, not 128-bit DWCAS

- Critical section is ~4 instructions; lock-free CAS would not measurably improve throughput.
- Spinlock is portable (no `cmpxchg16b` / `casp` requirement).
- Easier to reason about correctness on the first implementation pass.

**Why pair `next_lsn` and `next_wal_offset` under one critical section instead of two `fetch_add`s:** the partial-order invariant (§3.2.3) requires that the relative ordering of `next_lsn` advances matches the relative ordering of `next_wal_offset` advances. Two independent atomics cannot guarantee this without a coordination mechanism that would effectively re-introduce serialization.

### 3.3 WAL buffer (v1: infinite cap)

- The active buffer is **infinite capacity** in v1 — advancing `A.next_wal_offset` (§3.2) never fails on size.
- Buffer memory is bounded by `sync_interval × write_rate × 2` (one active, one being flushed).
- Two buffers exist (ping-pong): the **active** buffer accepts new writes; the **standby** buffer is either FREE or being flushed. Each buffer's lifecycle is governed by the packed-header state machine in §3.5.1: FREE → ACTIVE → PENDING → FREE.
- The only swap triggers in v1 are:
  1. `SyncWAL` is called (§3.4).
  2. A structural switch fires from prewrite (§3.9.1) — WAL too big, WBM should flush, trim_history non-empty, or flush_scheduler non-empty.
- v2 will add a soft-cap-driven proactive swap (see §4.3 and §4.9).

### 3.4 SyncWAL protocol

SyncWAL is a *switcher* in the sense of §3.5.4 — it uses the same `close_drain_open` primitive as the structural switch path. The difference from a structural switch (§3.9.1) is that SyncWAL does **not** run `SwitchWAL` / `SwitchMemtable` / `TrimMemtableHistory` / `ScheduleFlushes`. It only flushes `A.bytes[]` synchronously, durably syncs the underlying WAL file, and wakes any concurrent SyncWAL waiters.

The caller's flush step integrates with the existing `nonblocking_wal_io` coordination (see §4.4 Case 4 for the full rationale): before flushing, it calls `PrepareForSync()` on the log (sets `IsSyncing==true`); after `SyncWithoutFlush()` it calls `FinishSync()` + `log_sync_cv_.SignalAll()`. No new `durable_lsn` atomic or new cv is introduced — the existing `IsSyncing()` + `log_sync_cv_` machinery (db/db_impl/db_impl.cc:1494, db/db_impl/db_impl.h:2413) is the serialization primitive.

```
SyncWAL():
  A = active_buffer_ptr.load(acquire)
  B = the_other_buffer

  status = close_drain_open(A, B)         // §3.5.4 — phases 1, 2, then phase 3
  if status == NOT_SWITCHER:
    // Another switcher already closed A.  Wait for that switcher's sync to
    // complete by waiting on log_sync_cv_ until IsSyncing() clears on the
    // log that A was attached to.  Same vocabulary as today's SyncWAL
    // waiter queue (db/db_impl/db_impl.cc:1477-1478).
    log_write_mutex_.Lock()
    while (log_for_A).IsSyncing():
      log_sync_cv_.Wait()
    log_write_mutex_.Unlock()
    return Status::OK()

  // We are the switcher.  close_drain_open has already published B as the new
  // active buffer in phase 3.  New writers are landing in B.  We now flush A
  // synchronously into the log::Writer and fsync it.
  log_write_mutex_.Lock()
  (log_for_A).PrepareForSync()            // sets IsSyncing==true
  log_write_mutex_.Unlock()

  // Flush A.bytes into the file and fsync without holding log_write_mutex_ —
  // this is the same pattern as today's nonblocking_wal_io SyncWAL at
  // db/db_impl/db_impl.cc:1519-1537.  With Case 4 resolved (modify
  // NonBlockingIOWritableFileWriter in place), A.bytes IS the writer's
  // flush_buf_ after the swap, so this step reduces to FlushSwappedBuffer +
  // SyncWithoutFlush.  See §4.10.
  FlushSwappedBufferEquivalent(A)         // write A.bytes to OS
  writable_file->SyncWithoutFlush()       // fsync — actually durable

  log_write_mutex_.Lock()
  MarkLogsSynced(...)                     // clears IsSyncing on log_for_A
  log_sync_cv_.SignalAll()
  log_write_mutex_.Unlock()

  // Recycle A as the next standby.
  A.next_wal_offset = 0                   // (defensive; phase 3 of next switch resets too)
  A.header.store({state: FREE, count: 0}, release)

  return Status::OK()
```

#### 3.4.1 Key properties

- **No `min_non_hole_lsn` involvement.** SyncWAL only needs `A.in_flight_count == 0` to know `A.bytes[]` is fully written. Because of the partial-order invariant in §3.2.3 combined with §3.2.4 cross-buffer monotonicity, when `count == 0` for a buffer, every byte slot has been written, so the buffer is hole-free as a byte sequence. (LSN-level holes between two unrelated writers are tracked separately by `min_non_hole_lsn` (§3.6) and only affect read visibility, not WAL durability.)

- **Durable LSN is well-defined.** LSNs in any one buffer form a contiguous range (§3.2.3 partial-order invariant + §3.2.4 cross-buffer monotonicity). The reported durable LSN after Sync is the max LSN of that range.

- **No `DBImpl::mutex_` acquisition on the fast path.** SyncWAL operates on `A.header`, `B.header`, `active_buffer_ptr`, the log's `IsSyncing` flag, and the WAL file writer. It takes `log_write_mutex_` briefly (twice — once to set `IsSyncing`, once to clear it and broadcast), but never `DBImpl::mutex_` on the common path. This matches today's nonblocking_wal_io baseline and is a meaningful improvement over the pre-nonblocking_wal_io SyncWAL, which contended on `mutex_`. (The final `ApplyWALToManifest` step still takes `mutex_` on a manifest-update path, same as today.)

- **Phase 3 happens before the actual flush.** New writers can claim in `B` while we are still appending `A`'s bytes to the WAL file. The flush of `A` and the writes into `B` proceed concurrently. This is the same parallelism the existing nonblocking_wal_io ping-pong machinery already provides — SyncWAL's protocol just plugs into it via `close_drain_open`.

#### 3.4.2 Concurrent SyncWAL callers collapse

If two threads call SyncWAL concurrently:

1. Thread T1's `close_drain_open` wins phase 1 (CAS state ACTIVE→PENDING on A).
2. Thread T2's `close_drain_open` loses phase 1 (sees state == PENDING), returns `NOT_SWITCHER`.
3. T2 waits on `log_sync_cv_` until `IsSyncing()` clears on the log A was attached to.
4. T1 completes phases 2, 3, then runs `PrepareForSync → FlushSwappedBuffer → SyncWithoutFlush → MarkLogsSynced + SignalAll`.
5. T2 wakes, sees `IsSyncing()==false`, returns success.

T2's success guarantees that everything that was in `A` at the moment of T1's phase-1 CAS is durable — which includes every write T2 could possibly have wanted synced (T2's own writes must have landed in A before T2 saw `state != ACTIVE`, because `A` was the active buffer at that moment). This matches today's SyncWAL waiter semantics (db/db_impl/db_impl.cc:1476-1478).

#### 3.4.3 Interaction with structural switch (§3.9.1)

Both SyncWAL and the structural switch path use the same `close_drain_open` primitive. If they race to close `A`:

- Whoever's CAS succeeds in §3.5.4 phase 1 is the switcher. The other returns `NOT_SWITCHER`.
- The structural switcher additionally takes `DBImpl::mutex_` between phase 2 and phase 3 to do `SwitchWAL` / `SwitchMemtable` / etc. SyncWAL does not.
- A structural switcher *also* effectively acts as a SyncWAL: by closing and draining `A` and (eventually) flushing `A` through `PrepareForSync → FlushSwappedBuffer → SyncWithoutFlush → FinishSync`, the structural switcher's flush makes A durable. So a SyncWAL caller losing the race to a structural switcher waits on the same `log_sync_cv_` condition.
- Conversely, a SyncWAL caller does **not** do any structural work. If a structural condition (WAL too big, etc.) was the reason a writer wanted to switch, and SyncWAL races in first, the structural condition is still set after SyncWAL returns. The structural-switch writer will re-enter prewrite, see the condition still set, and trigger another switch.

This is acceptable: at most one extra switch per race, and structural switches are rare relative to SyncWAL calls.

### 3.5 Per-buffer packed header `{state, in_flight_count}`

This section fully specifies the switch-state and in-flight-count machinery. It supersedes the earlier per-buffer-counter sketch and is the central mechanism the rest of the design hangs off.

#### 3.5.1 Layout

Each ping-pong buffer has one `std::atomic<uint64_t>` header, laid out as:

```
bits  0..47   in_flight_count   (48 bits — vastly more than any realistic writer fan-in)
bits 48..49   state             (2 bits; values below)
bits 50..63   reserved          (must be zero, room for future flags)
```

State values (2 bits):

- `FREE    = 0` — buffer is not in use; `bytes[]` is empty, `next_wal_offset == 0`. The next switcher may transition this buffer into `ACTIVE`.
- `ACTIVE  = 1` — buffer is the current active buffer; new in-flight claims are accepted.
- `PENDING = 2` — buffer is closed to new claims; existing in-flight writers still need to drain. The buffer's `bytes[]` is being filled by writers that claimed before the close, and will be flushed by the switcher once `in_flight_count` reaches 0.
- (value `3` is reserved, unused in v1.)

There are exactly two buffer objects in memory (ping-pong). A separate atomic pointer `std::atomic<Buffer*> active_buffer_ptr` always names the one that is currently `ACTIVE`. Writers read `active_buffer_ptr` to find the buffer to claim in. The switcher updates `active_buffer_ptr` as part of phase 3 of the close-drain-open sequence (§3.5.4).

The `lsn_offset_spinlock` from §3.2 is **separate** from the header. It is not part of any buffer; it serializes only the (`next_lsn`, `A.next_wal_offset`) pair-advance.

#### 3.5.2 Writer claim protocol

The writer's claim of an in-flight slot folds the state check into the same atomic CAS that increments the count. There is no separate state check anywhere else in the writer's path — not in the spinlock, not in the memtable insert, not in the WAL memcpy.

```
claim_slot() -> Buffer*:
  loop:
    A   = active_buffer_ptr.load(acquire)
    h   = A.header.load(acquire)
    if h.state != ACTIVE:
      // A switch is in progress. Two sub-cases:
      //   - state == PENDING: switcher is between phase 1 and phase 3 on A
      //   - state == FREE: should not happen when A == active_buffer_ptr; defensive
      // Either way, spin briefly and retry. The retry will eventually load a new
      // active_buffer_ptr (B), or A will return to ACTIVE if the switch is aborted
      // (not possible in v1 — switches never abort).
      cpu_relax()
      continue
    new_h = {state: ACTIVE, count: h.count + 1}
    if A.header.compare_exchange_weak(h, new_h, acq_rel, acquire):
      return A
    // CAS failed — either count changed (another writer claimed concurrently) or
    // state changed (switcher closed A between our load and our CAS). Loop retries.
```

The returned `A` is the buffer in which this writer holds the slot. **All subsequent steps** of the writer (lsn/offset pair-advance §3.2, memtable insert, WAL memcpy, Link_buf completion §3.6) operate against this captured `A`. The writer does not re-read `active_buffer_ptr` later. Even if a switch happens mid-way, this writer finishes in the old `A`; the switcher's drain in §3.5.4 phase 2 is what waits for it.

#### 3.5.3 Writer release protocol

```
release_slot(A):
  A.header.fetch_sub(1, release)   // decrements count; state bits unaffected
```

`fetch_sub(1)` on the full `uint64_t` only decrements the low 48 bits because there is no borrow into the state bits at 48..49 (count is always ≥ 1 when this is called, so subtracting 1 cannot underflow into the state bits).

`release_slot` is called after **all** of the following have completed:
- memtable insert (concurrent SkipList insert),
- WAL memcpy into `A.bytes[offset .. offset + size]`,
- Link_buf completion store and watermark advance (§3.6).

The switcher's drain (§3.5.4 phase 2) only completes when every writer's `release_slot` has been observed. This is what guarantees that, by the time the switcher proceeds to flush `A.bytes[]`, every byte has been written and every writer has logically "left" `A`.

#### 3.5.4 Switcher protocol: `close_drain_open(A, B)`

This is the single primitive shared by §3.4 (SyncWAL) and §3.9.1 (structural switch). Both use it; both wrap it differently. The primitive itself is independent of `DBImpl::mutex_` — callers acquire `mutex_` separately if their switch type needs it.

```
close_drain_open(A, B) -> {SWITCHER, NOT_SWITCHER}:
  // ----- Phase 1: close A -----
  loop:
    h = A.header.load(acquire)
    if h.state != ACTIVE:
      // Lost the race: another switcher (SyncWAL or structural) already closed A.
      wait_until_active_buffer_ptr_changes_off(A)
      return NOT_SWITCHER
    new_h = {state: PENDING, count: h.count}      // only the state field changes
    if A.header.compare_exchange_weak(h, new_h, acq_rel, acquire):
      break
    // CAS failed — either state changed (handled by next iter's check) or count
    // changed (another writer just claimed in A). Loop retries.
  // A is now {PENDING, h.count}. From this point on, every claim_slot() that
  // captures active_buffer_ptr == A will see PENDING and spin in §3.5.2.

  // ----- Phase 2: drain A -----
  while A.header.load(acquire).count != 0:
    cpu_relax()
  // A is now fully drained: every writer that ever held a slot in A has called
  // release_slot. A.bytes[] is fully written up to A.next_wal_offset, and no
  // further mutation of A.bytes[], A.next_wal_offset, or A.header is possible
  // until this switcher proceeds.

  // ----- Phase 3: prepare B and publish as the new active buffer -----
  // [callers may insert their own slow work here, before phase 3 — see below]
  assert B.header.load() == {state: FREE, count: 0}
  B.next_wal_offset = 0                            // plain store; B is privately owned
  B.header.store({state: ACTIVE, count: 0}, release)
  active_buffer_ptr.store(B, release)
  // Writers that were spinning in §3.5.2 will, on their next loop iteration, load
  // active_buffer_ptr = B and successfully CAS B.header.

  return SWITCHER   // caller still owns A — must flush A and transition A.header → FREE
```

**Caller responsibilities after `SWITCHER` is returned:**

1. Flush `A.bytes[0 .. A.next_wal_offset]` to the underlying WAL file (synchronously for SyncWAL, asynchronously for structural switches — see §3.4 and §3.9.1).
2. After the flush completes, store `A.header = {state: FREE, count: 0}`. `A` is now the standby buffer for the next switch.

**Where the slow structural work goes for §3.9.1:** structural switchers must run `SwitchWAL` / `SwitchMemtable` / `TrimMemtableHistory` / `ScheduleFlushes` *between phase 2 and phase 3* (after drain, before publishing B). This is because B's writes will be committed against the new memtable and the new WAL file, so those structures must exist and be wired up before any writer claims a slot in B. See §3.9.1 for the details.

**Losing the phase 1 race (`NOT_SWITCHER`):** another switcher already closed `A`. The loser waits until `active_buffer_ptr` moves off `A` (meaning the winning switcher reached phase 3 and published `B`), then returns. Semantics:

- For SyncWAL: the winning switcher's flush will durably persist everything that was in `A` at the moment of close, which includes everything our caller wanted synced. The loser's caller can return success immediately (modulo waiting for the actual flush — see §3.4 for the cv-wait or `durable_lsn` check used).
- For structural switchers: the winning switcher may or may not have resolved our triggering condition. The loser's caller goes back to prewrite and re-evaluates. If the condition is still set, it will trigger another switch on the next iteration.

#### 3.5.5 Why the state check is not needed inside `lsn_offset_spinlock`

An earlier draft used a separate boolean `switch_pending` with a re-check inside the spinlock. The packed-header design eliminates both. The reasoning:

1. The writer's claim is one atomic CAS on the packed header. Either both `state == ACTIVE` and `count++` succeed together, or neither does. There is no window in which a writer could "already have claimed a slot" but "not yet have checked state."

2. Once the CAS succeeds, the writer owns an in-flight slot in `A`. The switcher's drain (§3.5.4 phase 2) cannot complete until `A.count` reaches 0. Therefore the switcher cannot proceed to flush `A.bytes[]` while this writer is still advancing lsn/offset, inserting into the memtable, or memcpy'ing into `A.bytes[]`.

3. Consequently, inside the `lsn_offset_spinlock` body (§3.2.2), `A.next_wal_offset` and `A.bytes[]` are guaranteed stable for as long as the writer holds the slot. No state check inside the spinlock is needed.

4. The `lsn_offset_spinlock` body therefore reduces to four plain stores (and two plain loads) with no conditionals.

This is also why there is no "double-check" anywhere: the prewrite-time slow-path triggers (§3.8, §3.9) are *optimizations* that let a writer take the slow path early when it already knows it should, but they are not load-bearing for correctness. The single load-bearing check is the CAS in §3.5.2.

#### 3.5.6 Why not pack everything into one 128-bit DWCAS?

We could in principle put `next_lsn`, `A.next_wal_offset`, `state`, and `count` into a single 128-bit atomic and do the entire claim with one DWCAS. We do not, for the same reason §3.2.5 rejected DWCAS:

- DWCAS (`cmpxchg16b` on x86_64, `casp` on aarch64) is not portable to all platforms RocksDB supports.
- The current split — packed 64-bit header for `(state, count)` plus a 4-instruction spinlock for `(next_lsn, next_wal_offset)` — already avoids any race that DWCAS would prevent, while remaining portable.
- The packed header gives us the race-free claim/state-check in one atomic op without requiring the wider primitive.

In short: the 64-bit packed header is the cheapest portable encoding that makes the writer's claim atomic with respect to the switcher's close. We do not need (and do not pay for) a wider primitive.

### 3.6 `min_non_hole_lsn` — Link_buf completion tracking

Tracks which LSN ranges have completed memtable insert, using a circular link buffer (same structure as InnoDB 8.0's `Link_buf` in `log0buf.cc`). The link buffer is indexed by LSN; each slot stores the end LSN of the batch that starts at that position, forming a chain that the watermark walks forward.

#### 3.6.1 Data structure

```
std::atomic<uint64_t> links[kCapacity];       // circular ring, indexed by lsn % kCapacity
std::atomic<uint64_t> min_non_hole_lsn;       // watermark — all LSNs below this are complete
```

`kCapacity` = 64K (512KB). Sized to cover the maximum in-flight LSN range: `max_concurrent_writers × max_batch_lsn_count`. 64K is generous for any realistic workload.

#### 3.6.2 Writer completion protocol

After completing memtable insert + WAL memcpy, and before `release_slot` (§3.5.3):

```
// Writer completed batch [start_lsn, start_lsn + batch_count):
links[start_lsn % kCapacity].store(start_lsn + batch_count, release)
```

One atomic store. O(1). No per-thread registration, no thread lifetime management.

#### 3.6.3 Watermark advance (lock-free, opportunistic)

Each completing writer opportunistically tries to advance the watermark after storing its link:

```
advance_watermark():
  loop:
    pos  = min_non_hole_lsn.load(acquire)
    next = links[pos % kCapacity].load(acquire)
    if next == 0:
      break                                    // hole — next batch not yet complete
    if min_non_hole_lsn.compare_exchange_weak(pos, next, acq_rel, acquire):
      links[pos % kCapacity].store(0, release) // recycle slot for reuse
      continue                                 // keep walking — maybe next slot is also done
    else:
      break                                    // another thread is advancing, let it
```

Properties:
- **Lock-free.** Progress is guaranteed as long as any completing writer runs. No mutex, no registration, no per-thread state.
- **O(completed batches) per advance.** Each hop skips an entire batch's LSN range (one hop per batch, not per LSN). Worst case is ~`max_concurrent_writers` hops per full advance pass.
- **Opportunistic.** Multiple writers may attempt to advance concurrently; the CAS serializes them. Losers simply stop — the winner carries the watermark forward.
- **`min_non_hole_lsn` is monotonically non-decreasing.** The CAS only moves it forward.

#### 3.6.4 Oversize batch rejection and ring-full abort

Two checks, at different points in the pipeline:

**Check 1 — prewrite (§3.8), before the §3.5 CAS claim:**

```
if batch_count >= kCapacity:
  return Status::Busy("WriteBatch too large for link buffer")
```

A single batch with `batch_count >= kCapacity` (64K+ keys) would cause `next_lsn` to jump past the ring capacity, colliding in the modular index. Rejected before the point of no return. The caller can split the batch or use the regular group-commit path.

**Note:** `kCapacity = 64K` is a hard limit on single-batch size in this write path. This should be documented and commented prominently in the implementation.

**Check 2 — after §3.2 pair-advance, before memtable insert:**

```
if start_lsn + batch_count - min_non_hole_lsn.load(acquire) >= kCapacity:
  abort()  // catastrophic: cumulative in-flight range exceeds ring capacity
```

This fires only when many concurrent writers' cumulative in-flight LSN range exceeds 64K — meaning some writer has been stuck for an extraordinary duration. Aborting is consistent with §3.11's error policy.

#### 3.6.5 Interaction with §3.1 pipeline

The link store (§3.6.2) and watermark advance (§3.6.3) happen at step 7 of the §3.1 pipeline, after memtable insert and WAL memcpy, and before `release_slot` (step 8). This ordering ensures that when `min_non_hole_lsn` advances past a batch's LSN range, the batch's memtable entries and WAL bytes are fully written.

### 3.7 Read visibility

- `GetSnapshot()` pins `snapshot_lsn = next_lsn - 1` at the time of the call. This is a plain atomic load — **no wait**.
- `Get` and iterators that do not specify an explicit snapshot pin `snapshot_lsn = next_lsn - 1` implicitly at the start of the read.
- **Deferred wait:** on the first actual read under a snapshot, the reader waits until `min_non_hole_lsn >= snapshot_lsn`. This guarantees that every write with `lsn <= snapshot_lsn` has completed its memtable insert, so the snapshot is consistent. `min_non_hole_lsn` is monotonically non-decreasing, so the wait triggers at most once per snapshot — subsequent reads under the same snapshot proceed without waiting.
- **Read-your-own-write is preserved.** A thread that calls `Put` (assigned LSN N) and then calls `Get` will pin `snapshot_lsn >= N`. The deferred wait ensures `min_non_hole_lsn` reaches N before the read executes, so the thread's own write is visible.
- **Wait bound:** the wait is bounded by the time for the slowest in-flight writer to complete its memtable insert + WAL memcpy, which is typically microseconds.

### 3.8 Prewrite — fast path is per-thread, lockless

Each writer runs the equivalent of `DBImpl::PreprocessWrite`'s checks **per-thread, with no leader**. The criterion: any check that does not take a lock can run per-thread; lock-taking handlers go through the slow-path serializer.

All condition tests in current `PreprocessWrite` (db/db_impl/db_impl_write.cc:1114) are lockless atomic / concurrent reads:

| Check | Test | Lockless? |
|---|---|---|
| DB stopped | `error_handler_.IsDBStopped()` | yes |
| WAL too big | `total_log_size_ > GetMaxTotalWalSize()` | yes |
| WBM should flush | `write_buffer_manager_->ShouldFlush()` | yes |
| TrimHistory pending | `!trim_history_scheduler_.Empty()` | yes (concurrent queue) |
| Pending flushes | `!flush_scheduler_.Empty()` | yes (concurrent queue) |
| Stall / delay | `write_controller_.IsStopped() \|\| NeedsDelay()` | yes |
| WBM should stall | `write_buffer_manager_->ShouldStall()` | yes |
| `need_log_sync` | local field | yes |

Additionally, prewrite rejects batches that exceed the Link_buf ring capacity (§3.6.4 Check 1):

| Check | Test | Lockless? |
|---|---|---|
| Batch too large for Link_buf | `batch_count >= kCapacity` | yes |

When **none** of the conditions fire (the common case), prewrite is a sequence of atomic reads with no locks. The writer falls through directly to §3.2.

**Prewrite must run strictly before the (lsn, offset) claim.** Anything that could fail or wait must happen here, because after the claim there is no rollback path.

### 3.9 Slow-path serializer

Prewrite failures split into **two disjoint slow paths** depending on what the failing check needs. Structural switches need a drain; stalls/waits do not. They never share state.

#### 3.9.1 Structural switch path (drain required)

Triggered by: WAL too big, WBM should flush, non-empty trim_history, non-empty flush_scheduler. These handlers mutate memtable/WAL structure (`SwitchWAL` rewires the WAL file pointer; `SwitchMemtable` makes the current memtable immutable and installs a new one; etc.) and must not run concurrently with in-flight writers that are still writing into the old memtable / old WAL buffer.

The structural switch path uses the same `close_drain_open` primitive from §3.5.4, but **inserts the structural work between phase 2 (drain) and phase 3 (publish B)**, all under `DBImpl::mutex_`. New writers cannot land in `B` until `B` is published, which happens only after the structural rewire is complete — so the new memtable and the new WAL file are guaranteed to be in place before any writer commits against them.

```
structural_switch_path():
  A = active_buffer_ptr.load(acquire)
  B = the_other_buffer

  // ----- Phase 1 of §3.5.4: close A -----
  loop:
    h = A.header.load(acquire)
    if h.state != ACTIVE:
      // Lost the race — another switcher (SyncWAL or another structural) is on it.
      wait_until_active_buffer_ptr_changes_off(A)
      return RETRY_PREWRITE
    if A.header.compare_exchange_weak(h, {state: PENDING, count: h.count}, ...):
      break

  // ----- Phase 2 of §3.5.4: drain A -----
  while A.header.load(acquire).count != 0:
    cpu_relax()
  // A.bytes[], A.next_wal_offset, and the memtable A's writers were targeting
  // are now stable. Safe to do structural rewire.

  // ----- Structural work (between phase 2 and phase 3) -----
  acquire DBImpl::mutex_
    // Coalesce all structural handlers triggered by prewrite. We do this under
    // ONE mutex_ acquisition; we do NOT drop and re-acquire mutex_ for each.
    if WAL too big:           SwitchWAL(...)
    if WBM should flush:      HandleWriteBufferManagerFlush(...)
    if !trim_history.Empty(): TrimMemtableHistory(...)
    if !flush_sched.Empty():  ScheduleFlushes(...)
    // SwitchWAL installs a new on-disk WAL file; SwitchMemtable installs a new
    // memtable. After this block, the DB's "current WAL file" and "current
    // memtable" point at fresh, empty structures that B's writers will commit to.
  release DBImpl::mutex_

  // ----- Phase 3 of §3.5.4: publish B -----
  assert B.header.load() == {state: FREE, count: 0}
  B.next_wal_offset = 0
  B.header.store({state: ACTIVE, count: 0}, release)
  active_buffer_ptr.store(B, release)
  // New writers can now claim in B. They will commit to the NEW memtable and
  // their bytes will eventually be flushed to the NEW WAL file.

  // ----- Background flush of A -----
  // Schedule an async flush that writes A.bytes[0 .. A.next_wal_offset] to
  // A's WAL file (the OLD one, before SwitchWAL ran) via the standard
  // IsSyncing protocol:
  //   log_write_mutex_.Lock()
  //   (log_for_A).PrepareForSync()            // IsSyncing==true
  //   log_write_mutex_.Unlock()
  //   FlushSwappedBufferEquivalent(A)
  //   writable_file->SyncWithoutFlush()
  //   log_write_mutex_.Lock()
  //   MarkLogsSynced(...); log_sync_cv_.SignalAll()
  //   log_write_mutex_.Unlock()
  //   A.header = {state: FREE, count: 0}
  // This is the same protocol SyncWAL uses (§3.4), just invoked from a
  // background task.  Every existing waiter on IsSyncing (SwitchMemtable,
  // LockWAL, SyncClosedLogs, FindObsoleteFiles) is correctly serialized
  // against this background flush for free.  The triggering writer does NOT
  // wait for the async flush to complete.
  schedule_async_flush(A)

  return RETRY_PREWRITE   // the triggering writer retries prewrite and re-claims
```

**Phase ordering rationale.** The structural rewire **must** happen between phase 2 and phase 3:

- *Not before phase 2:* if we did the rewire while writers were still in flight in `A`, those writers might commit against the new memtable mid-write, breaking memtable invariants.
- *Not after phase 3:* if we published `B` before rewiring, new writers would land in `B` and commit against the *old* memtable / *old* WAL file, then `SwitchWAL` would yank the WAL file out from under them.

Inserting the rewire between phase 2 and phase 3 is the only correct place.

**Async flush of A targets the OLD WAL file.** This is important. `A`'s bytes were accumulated when the OLD WAL file was current. When the async flush appends `A.bytes[]`, it must append to the OLD WAL file, not the new one. This is straightforward to implement: each buffer object holds a pointer to the WAL file writer it was attached to, captured at the moment the buffer became ACTIVE (or, equivalently, captured when phase 3 of the previous switch ran). The flush uses that captured writer.

**Coalescing.** Multiple structural handlers share **one** drain barrier (phase 2), **one** `mutex_` acquisition (the structural-work block), and **one** buffer roll (phases 1+3). We do not drain or roll N times for N triggered conditions.

**Multiple writers racing to be the switcher.** Resolved by phase 1 of §3.5.4: the CAS on `A.header` from ACTIVE→PENDING. Only one writer's CAS succeeds; the others receive `NOT_SWITCHER`, wait for `active_buffer_ptr` to move off `A`, and re-enter prewrite. If their triggering condition was resolved by the winning switcher's structural work, they proceed normally on the next pass. If not (e.g., the winning switcher was a SyncWAL caller that did not run structural handlers), they trigger another switch.

**`switch_pending` is gone.** There is no separate boolean flag, no separate cv. The `state` field in `A.header` *is* the switch-pending signal — it transitions ACTIVE→PENDING during phase 1 and only returns to FREE much later, after the async flush of A completes. Writers observe it atomically as part of their claim CAS in §3.5.2.

**New writers during the switch.** Writers that capture `active_buffer_ptr == A` after phase 1 closed it but before phase 3 published `B` will spin in §3.5.2 (`A.state == PENDING`, CAS fails, retry). The spin window is bounded by:

- Phase 2 drain time (typically microseconds — wait for in-flight writers to finish their memtable + memcpy work).
- Time to acquire `DBImpl::mutex_` (variable; depends on contention).
- Structural work duration (`SwitchWAL` is fast; `SwitchMemtable` allocates a memtable; `ScheduleFlushes` enqueues background work — overall typically milliseconds).
- Phase 3 publication (a few atomic stores).

For typical workloads this window is dominated by the `mutex_` + structural work and is comparable to today's structural switch latency. Accepted for v1.

**Note on `WaitForPendingWrites`.** The existing function `DBImpl::WaitForPendingWrites` is a **literal no-op** when `unordered_write=false && enable_pipelined_write=false` (verified at db/db_impl/db_impl.h:1926). The drain in `close_drain_open` phase 2 is a **new** mechanism, not a replacement of `WaitForPendingWrites`. The new drain is needed because the new path **removes the WriteThread leader/group serialization** that previously made no-drain safe.

#### 3.9.2 Stall / wait path (no drain)

Triggered by: `write_controller_.IsStopped()`, `NeedsDelay()`, `WriteBufferManager::ShouldStall()`. These are pure waits — they do not structurally mutate memtable/WAL, so in-flight writers can keep landing in the active buffer while the stalled writer sleeps.

```
stall_path():
  acquire DBImpl::mutex_
    wait on stall cv until condition clears (DelayWrite / WriteBufferManagerStallWrites equivalent)
  release DBImpl::mutex_
  // writer goes back to prewrite and retries
```

**No `switch_pending`. No in-flight drain. No coordination with the switch path.** A stalled writer holds `mutex_` only while sleeping on the cv and does not block independent writers on the fast path — the fast path never touches `mutex_`.

**If both a stall condition and a switch condition fire in the same prewrite pass:** handle the switch path first (it advances WAL/memtable state and may clear the stall), then retry prewrite. Stall path runs only when prewrite sees a stall *and no switch condition*.

**Note on `WaitForPendingWrites`:** the existing function `DBImpl::WaitForPendingWrites` is a **literal no-op** when `unordered_write=false && enable_pipelined_write=false` (verified at db/db_impl/db_impl.h:1926). The drain in the switch path is a **new** mechanism, not a replacement of `WaitForPendingWrites`. The reason it is needed is that the new path **removes the WriteThread leader/group serialization** that previously made no-drain safe.

### 3.10 Memtable switch and WAL switch are coupled

- A memtable switch always rolls a new WAL (matches current behavior with `two_write_queues=false`).
- A WAL switch always rolls a new (fresh, empty) buffer.
- One switch operation, one drain, one new buffer.

### 3.11 Error policy: abort on any IO error

- Any IO error encountered after the §3.5 in-flight claim is held (i.e., anywhere in steps 4–8 of the §3.1 pipeline: lsn/offset advance, memtable insert, WAL memcpy, Link_buf completion, or `release_slot`) → process abort. Once the §3.5 CAS has succeeded the write is committed; there is no rollback path.
- Any IO error in `SyncWAL`'s flush (§3.4) → process abort.
- Any IO error in the structural switch path's async flush of A (§3.9.1) → process abort.
- This eliminates the "user was told success but later sync failed" semantic regression that the asynchronous flush model would otherwise create.
- Consistent with the existing nonblocking_wal_io durability model, which already accepts ~100ms data loss on process crash.

---

## 4. Open items and resolutions

Items 4.1, 4.2, 4.4 (all cases), 4.5, 4.6, 4.7, 4.8 are resolved. Items 4.3 and 4.9 are deferred to v2. Item 4.10 is the remaining work — the component breakdown for the implementation pass.

### 4.1 Stall handling — RESOLVED

**Decision:** stalls stay in a **separate wait zone** from the structural switch path. Stall path acquires `mutex_`, sleeps on the stall cv, and retries prewrite — no `switch_pending`, no in-flight drain, no interaction with the switch path. `TrimMemtableHistory` is treated as a structural handler (it mutates memtable history) and runs under the switch path, not the stall path.

Detailed in §3.9.1 (switch path, with drain) and §3.9.2 (stall path, no drain). Open sub-question on ordering when both stall and switch conditions fire in the same prewrite pass: resolved as "handle switch first, then retry prewrite."

**Status:** resolved 2026-04-10.

### 4.2 Double-check `switch_pending` inside the claim spinlock — RESOLVED

**Resolution:** `switch_pending` is **eliminated entirely**, and the "double-check" is gone with it. The packed per-buffer header in §3.5 supersedes both:

- **State and in-flight count are packed into one `std::atomic<uint64_t>` per buffer** (§3.5.1).
- **The writer's claim is one atomic CAS** that simultaneously checks `state == ACTIVE` and increments `count` (§3.5.2). There is no window in which a writer could have "claimed a slot" without "checking state" — the two happen together or neither happens.
- **The `lsn_offset_spinlock` (§3.2) carries no state check.** Once the §3.5.2 CAS succeeds, the switcher's drain (§3.5.4 phase 2) is guaranteed to wait for this writer's `release_slot`, so the buffer's bytes and offset are stable for the entire duration of the spinlock body. The spinlock body is therefore four plain stores and two plain loads — no conditionals, no branches.
- **The switcher's close-drain-open primitive** (§3.5.4) is also race-free: phase 1 is a CAS on `A.header` that flips `state` while preserving `count`, and phase 2 drains by polling `count`. No separate flag, no separate cv.

The barrier question is answered by the packed-header CAS. Detailed explanation of why no in-spinlock check is needed is in §3.5.5; rationale for the choice of 64-bit packed header over 128-bit DWCAS is in §3.5.6.

**Status:** resolved 2026-04-10.

### 4.3 v2 — WAL buffer overflow handling

v1 uses infinite-cap. v2 needs:

- **Soft cap** on the active buffer; when exceeded, trigger a proactive swap.
- **Coordination model:** writer-triggered swap (the writer that detects overflow becomes responsible for swapping) vs. SyncWAL-only swap (which would require frequent SyncWAL calls).
- Tentative: writer-triggered swap, mirroring how prewrite-driven WAL switch works.

**Status:** explicitly deferred to v2.

### 4.4 Recovery audit — WAL file order

**Current analysis:** WAL records are guaranteed to be in monotonic LSN order in the WAL file, on two levels:

- **Within a single buffer:** the partial-order invariant (§3.2.3) ensures that, for any two writers in the same buffer, lsn ordering matches in-buffer offset ordering. So when the buffer's bytes are appended to the WAL file (in offset order), they are also in lsn order.
- **Across a buffer boundary (A → B):** the close-drain-open sequence in §3.5.4 ensures that all `A`-writers complete their lsn advance before `B` is published as the new active buffer. By the time the first `B`-writer claims, every advance of `next_lsn` by an `A`-writer has already happened. Therefore `max(lsn in A) < min(lsn in B)`. See §3.2.4 for the full argument.

The WAL file flush order is also `A` then `B` (the §3.5.4 `SWITCHER` caller flushes `A` after publishing `B`; `B` is only flushed in some later switch). Combined, recovery sees a WAL file that is monotonic in lsn end-to-end.

**Status:** analysis complete on the core invariants. Four edge cases audited below, all resolved.

#### Case 2 — Partial buffer flush on crash — RESOLVED

RocksDB's existing `log::Reader` already handles torn WAL tails via per-record CRC framing; `wal_recovery_mode` (default `kPointInTimeRecovery`) stops replay at the last valid record. Nothing about this assumes *how* the bytes were appended — one big `Append(A.bytes)` vs. many `AddRecord` calls is indistinguishable to the reader.

**Correctness obligation the new path inherits:** each writer's memcpy into `A.bytes[]` must produce bytes that are wire-format identical to what `log::Writer::AddRecord` would emit — same 7-byte header, same CRC, same 32KB-block fragmentation (FIRST/MIDDLE/LAST splits). That requires the per-writer serializer to know its absolute WAL-file offset (`file_offset_at_A_start + A_offset_for_this_writer`), not just its offset within `A.bytes[]`, so block-boundary splits land on the right boundaries. This is an implementation detail for §3.10 / §4.10, not a design change.

#### Case 3 — SwitchMemtable quiescence vs. a background flush of a prior buffer — RESOLVED

The existing coordination flag `LogWriterNumber::getting_synced` (exposed as `IsSyncing()`) is set by `PrepareForSync()` before `SwapBuffer()` and cleared by `FinishSync()` after `SyncWithoutFlush()`. `SwitchMemtable` (db/db_impl/db_impl_write.cc:2132-2143), `LockWAL` (db/db_impl/db_impl.cc:1595-1607), `SyncClosedLogs` (db/db_impl/db_impl_compaction_flush.cc:92-93), and `FindObsoleteFiles` (db/db_impl/db_impl_files.cc:315-316) all wait on `IsSyncing()` before touching the log::Writer's `buf_`.

The new path's async flush of `A` runs through the same `PrepareForSync → FlushSwappedBuffer-equivalent → SyncWithoutFlush → FinishSync + SignalAll` sequence. Every existing waiter therefore covers the new path's async flush for free — no new flag, no new cv, no new code paths in compaction/file-cleanup. §3.9.1's `schedule_async_flush` pseudocode makes this explicit.

#### Case 4 — Layering on `NonBlockingIOWritableFileWriter` — RESOLVED

The new design has its own WAL-record-level ping-pong (buffers A, B with `A.bytes[]`). `NonBlockingIOWritableFileWriter` also has a ping-pong (`buf_` ↔ `flush_buf_`). Stacking both is redundant and creates two points where unflushed bytes can live.

**Decision:** modify `NonBlockingIOWritableFileWriter` **in place** for now. The existing `buf_` / `flush_buf_` pair *becomes* the A/B pair; we extend the class with the packed `{state, in_flight_count}` header (§3.5.1), the `next_wal_offset` per buffer, and the close-drain-open-aware swap. No subclass, no separate new-path-only writer. Once the implementation stabilizes (interfaces are clean, tests are in place, no churn), we can revisit: either extract a subclass for the eliminate-group-commit mode, or keep the merged class if it generalizes cleanly. Deferring the refactor avoids designing an interface against an unsettled implementation.

**Why this works.** `NonBlockingIOWritableFileWriter` today is small (~100 lines across `.h` and `.cc`) with two public primitives (`SwapBuffer`, `FlushSwappedBuffer`) and a well-understood invariant (`flush_buf_` non-empty only between swap and flush-complete). The new path needs: (a) per-buffer state machine, (b) per-buffer offset, (c) writer-claim CAS, (d) drain. All of these are additions on top of the existing fields; none conflict with the existing contract. The existing SyncWAL/LockWAL/SwitchMemtable waiters on `IsSyncing()` continue to work unchanged — our async flush calls `PrepareForSync()` / `FinishSync()` just like today's SyncWAL does.

**Contract the modification must preserve.** In the default mode (`nonblocking_wal_io=true` but not eliminate-group-commit, i.e., still going through the WriteThread leader), `NonBlockingIOWritableFileWriter` must behave exactly as today. The new fields default to benign values (state=ACTIVE from construction time, in_flight_count=0, next_wal_offset=0 that the non-eliminate path simply doesn't read). Tests for `nonblocking_wal_io` on master must continue to pass unchanged.

**Status:** resolved 2026-04-14 — modify in place, revisit after stabilization.

#### Case 1 — SyncWAL + structural switch race on the same buffer — RESOLVED (conditional on Case 4 decision)

The design's §3.4 (SyncWAL) and §3.9.1 (structural switch) both use `close_drain_open`. Phase 1's CAS on `A.header` ensures exactly one wins; the loser returns `NOT_SWITCHER`. This is race-free at the buffer level.

With Case 4 resolved (modify `NonBlockingIOWritableFileWriter` in place), the log::Writer-level interaction is handled by the existing `IsSyncing()` + `log_sync_cv_` machinery:

- Both the new SyncWAL path and the structural-switch's async flush drive `PrepareForSync()` / `SwapBuffer()` / `FlushSwappedBuffer()` / `SyncWithoutFlush()` / `FinishSync()` on the underlying `NonBlockingIOWritableFileWriter`. Only one can hold `IsSyncing==true` at a time; concurrent callers queue via `log_sync_cv_`. This is the same serialization the `nonblocking_wal_io` baseline already uses for concurrent `SyncWAL` callers (db/db_write_test.cc:859 exercises this).
- A SyncWAL loser in the A/B close-drain-open race that saw `state != ACTIVE` on `A.header` must still wait for `A`'s durability (not just for `active_buffer_ptr` to move off `A`). That translates to waiting on `log_sync_cv_` until the winner's `FinishSync()` fires. This replaces the `durable_lsn` cv sketched in §3.4 — no new primitive is needed; the existing log::Writer-level cv suffices. §3.4's pseudocode should be updated accordingly.
- Across a log::Writer boundary (a structural switcher did `SwitchWAL`, the new log is now current), a SyncWAL caller that wanted bytes from the **old** log must walk `logs_` the way today's SyncWAL does (db/db_impl/db_impl.cc:1491-1506 iterates all logs with `number <= current_log_number`). §3.4's new protocol must adopt this iteration rather than assuming a single-log shortcut.

**Residual spec work.** §3.4 still carries a sketched `durable_lsn` atomic and a custom wait primitive; with Case 4 resolved those should be removed in favor of `log_sync_cv_` + `IsSyncing()`. Left as a spec-polish task for the implementation pass — it does not change correctness, only the vocabulary the doc uses.

**Status:** resolved 2026-04-14.

### 4.5 WriteBatch validation timing — RESOLVED

**Where validation lives today.** Two places on the leader's side of the group-commit path:

1. `DBImpl::MergeBatch` (db/db_impl/db_impl_write.cc:1224) walks the write group and calls `WriteBatchInternal::Append(*merged_batch, writer->batch, /*WAL_only*/true)` for each follower. `Append` implicitly validates the batch (parses the header, rejects malformed batches) and the first failure fails the whole group.
2. `DBImpl::WriteToWAL` (db/db_impl/db_impl_write.cc:1280) calls `merged_batch.VerifyChecksum()` once on the merged batch, right before `log_writer->AddRecord`.

Already per-thread and unaffected: `WriteCallback` (pre-merge), and `protection_bytes_per_key` per-KV verification at memtable insert time.

**Where validation moves in the new path.** `MergeBatch` disappears entirely — no merging, each writer writes its own bytes. Both validations collapse into per-writer prewrite, **before the §3.5 packed-header CAS:**

1. `batch->VerifyChecksum()` on the writer's own batch. This is the same call today's `WriteToWAL` makes on the merged batch, just moved to prewrite and scoped to one batch.
2. Batch format sanity — equivalent to what `WriteBatchInternal::Append` would have caught. A dedicated helper (e.g., `WriteBatchInternal::CheckContents(batch)` that parses the header and walks records without copying) is the cleanest option; at minimum a debug-only `Append` to a scratch buffer catches regressions.

Both are lockless, per-thread atomic/concurrent reads — they fit §3.8 trivially. They run **before** the §3.5 CAS because the CAS is the point of no return: once a writer owns an in-flight slot, the design has no rollback (§3.11). A validation failure in prewrite is returned to the caller as a regular `Status` without ever touching buffer state.

**What the implementation must guarantee.** No side effects before the §3.5 CAS: validation must not advance `next_lsn`, must not touch `A.header` or `A.bytes[]`, must not write to the Link_buf ring. On failure, the writer returns the status and the caller sees exactly what it sees today on a validation failure.

**Status:** resolved 2026-04-14.

### 4.6 `min_non_hole_lsn` — concrete slot encoding — RESOLVED

**Resolution:** per-thread slots replaced by a Link_buf (§3.6). Each slot in the circular ring stores the end LSN of the completed batch at that position. No per-thread encoding needed — the ring is indexed by LSN, not by thread.

**Status:** resolved 2026-04-10.

### 4.7 Per-thread slot registry — lifetime and allocation — RESOLVED

**Resolution:** eliminated. The Link_buf (§3.6) requires no per-thread registration, no thread lifetime management, and no slot allocation. Writers store into the ring at their LSN index; the watermark advance is lock-free and opportunistic. Works with any threading model (thread pools, dynamic threads, fibers).

**Status:** resolved 2026-04-10.

### 4.8 `WriteController` interaction with per-thread prewrite — RESOLVED

**Decision:** the stall path (§3.9.2) is acceptable as-is. `WriteController::IsStopped()` and `NeedsDelay()` are atomic and lockless, so the fast-path check in prewrite does not contend. When a writer actually stalls, it falls into the slow path and serializes through `DBImpl::mutex_` + the stall cv. That serialization is acceptable because stall is an already-degraded mode — throughput is not the concern; correctness and liveness are.

**Correctness contract the implementation must uphold:**

- **No deadlock.** A stalled writer holds `mutex_` only while sleeping on the cv. It must not hold any buffer slot (it has not yet run the §3.5 CAS), must not hold `lsn_offset_spinlock`, and must not block a switcher's drain. Fast-path writers that are already past §3.5 must be able to complete and release their slots while other writers are stalled.
- **No crash.** The stall path must not touch buffer state, `next_lsn`, `next_wal_offset`, or the Link_buf ring. It is a pure wait; on wakeup the writer re-enters prewrite from the top and re-claims a slot normally.
- **Stall + switch interaction.** Per §3.9.2, if a stall condition and a switch condition fire in the same prewrite pass, the switch path runs first. A writer that sees only a stall condition waits on the cv; when the stall clears (via a flush completion or WriteController state change), it retries prewrite — possibly discovering a switch condition this time, which it then handles through §3.9.1.

**Unit tests required:**

1. **Stall does not deadlock the fast path.** Put N writers into the stall path (e.g., force `WriteBufferManager::ShouldStall()` to return true), while M other writers are on the fast path. The fast-path writers must complete. The stalled writers must wake and complete after the stall condition clears.
2. **Stall + concurrent switcher.** Trigger a structural switch (WAL rotate) while writers are stalled. The switcher must be able to complete phase-1/2/3 and the flush of `A` without needing the stalled writers to wake.
3. **Stall + concurrent SyncWAL.** A SyncWAL caller must complete (close_drain_open + flush) while stalled writers are asleep on the cv. Stalled writers never held a slot, so drain is trivially satisfied; the test pins this assumption.
4. **Stall then switch condition.** A writer that wakes from stall and, on retry, sees a switch condition must correctly route through §3.9.1 — no crash, no double-claim.
5. **Many stalled writers wake simultaneously.** On broadcast, all N wake, re-run prewrite concurrently, and either claim slots (fast path) or re-stall. Verify no crashes, no lost wakeups, no Link_buf corruption.
6. **WriteController::IsStopped toggled under load.** Flip the stop flag mid-run; in-flight fast-path writers must be allowed to finish (they passed prewrite already), new writers must stall; on un-stop, stalled writers must wake.

These tests must live in the new-path-specific test file and be run under TSAN.

**Status:** resolved 2026-04-14.

### 4.9 Soft cap on infinite buffer (v1 contract)

If the application stops calling SyncWAL (bug, deadlock, slow consumer), the active buffer grows unbounded.

**Decision (v1):** accepted. "App must call SyncWAL regularly" is part of the v1 contract.
**v2:** add an internal SyncWAL trigger when the buffer crosses a soft threshold.

**Status:** v1 stance confirmed; v2 mechanism not specified.

### 4.10 Components to add / modify — see companion doc

The implementation ordering, component breakdown, and PR sequencing live in `eliminate_group_commit_impl_plan.md` (Round 1 independent building blocks, Round 2 integration with the core write path). That doc cross-references each section of this design.

---

## 5. Out of scope for v1 (explicit non-goals)

- WAL buffer overflow handling (deferred to v2)
- Alternative `min_non_hole_lsn` structures (bitmap-ring, per-thread slots) if Link_buf proves insufficient
- 2PC support
- `WriteOptions::sync` support
- `WriteOptions::disableWAL` support
- Coexistence with `unordered_write`
- Coexistence with `enable_pipelined_write`
- WriteBatch retry / error recovery (we abort on IO error)

---

## 6. Pointers to existing context

- Existing nonblocking_wal_io feature on branch `work-from-7.7.fb`. Relevant commits:
  - `3e20b442f nonblocking_wal_io: non-blocking WAL write path with ping-pong buffer`
  - `cd887ff05 nonblocking_wal_io: harden with asserts, fix RangeSync, and cleanups`
  - `16e785dd9 nonblocking_wal_io: revert to mutex-based buffer swap, fix BufferSize race`
  - `e1b6873c9 nonblocking_wal_io: reject sync=true writes, merge SyncWAL, add invariants`
  - `88f54677f nonblocking_wal_io: reject WAL checksum handoff, harden subclass`
  - `3efbf64da nonblocking_wal_io: abort on irrecoverable FlushSwappedBuffer failure in SyncWAL`
- See project memory `project_nonblocking_wal.md` for design notes.
- Key files in the nonblocking_wal_io baseline:
  - `file/nonblocking_io_writable_file_writer.{h,cc}` — `SwapBuffer()` and `FlushSwappedBuffer()` primitives.
  - `DBImpl::SyncWAL` — db/db_impl/db_impl.cc:1494 (uses `PrepareForSync()` → `SwapBuffer()` → release `log_write_mutex_` → `FlushSwappedBuffer()` + `SyncWithoutFlush()` → `MarkLogsSynced()`).
  - `DBImpl::WriteToWAL` — db/db_impl/db_impl_write.cc:1290 (under `log_write_mutex_` when `nonblocking_wal_io_`).
  - `DBImpl::SwitchMemtable` — db/db_impl/db_impl_write.cc:2011 (waits on `IsSyncing()` before `WriteBuffer()` when `nonblocking_wal_io_`, db_impl_write.cc:2132-2143).
  - `DBImpl::LockWAL` — db/db_impl/db_impl.cc:1588 (waits on `IsSyncing()` before `WriteBuffer()`).
  - `LogWriterNumber::IsSyncing()` — db/db_impl/db_impl.h:1609; set by `PrepareForSync()`, cleared by `FinishSync()`.
  - `log_sync_cv_` — db/db_impl/db_impl.h:2413; broadcast at `MarkLogsSynced` and `MarkLogsNotSynced`.
- Reference points in current code:
  - `DBImpl::PreprocessWrite` — db/db_impl/db_impl_write.cc:1114
  - `DBImpl::WaitForPendingWrites` — db/db_impl/db_impl.h:1926 (no-op for our config)
  - `DBImpl::WriteImpl` — db/db_impl/db_impl_write.cc (group commit entry)
