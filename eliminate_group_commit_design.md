# Eliminate Group Commit — Design Notes

**Status:** Design discussion in progress. Last updated 2026-04-15.
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

*Implementation note (2026-04-15):* live in `db/db_impl/db_impl_write_no_group.cc :: DBImpl::WriteImplNoGroup`. Dispatched from `WriteImpl()` when `use_group_commit=false`. Reader visibility is published via `VersionSet::SetLastSequenceAtLeast(Link_buf::AdvanceWatermark() - 1)` — a CAS-max loop driven by the monotonic watermark return (§3.6), not by `SetLastSequence` (which asserts monotone input that out-of-order writer completion would violate).

Each writer thread executes the following sequence with no leader and no group:

```
WriteImpl_NoGroup(batch):
  1. Run per-thread prewrite (lockless checks; see §3.8)
  2. If any prewrite check fires → enter slow-path serializer (§3.9.1 or §3.9.2)
  3. Under wal_write_mutex_ (adaptive):
       - snapshot logs_.back() -> log_writer / file_writer
       - slot = file_writer->ClaimSlot()            // bumps in_flight_count
       - start_lsn     = next_lsn_
       - start_offset  = slot->next_wal_offset
       - abs_offset    = slot->file_offset_start + start_offset
       - byte_count    = log::ComputeSerializedRecordSize(payload, abs_offset, recycle)
       - next_lsn_           += batch_lsn_count
       - slot->next_wal_offset += byte_count
  4. Link_buf ring-capacity guard (§3.6.4 check 2)
  5. Insert batch into memtable (concurrent SkipList insert, mutex-free)
  6. SerializeRecord into SlotBytesStart(slot) + start_offset  (§3.10)
  7. file_writer->AdvanceFileSize(byte_count)      // keep filesize_ honest
  8. Link_buf->Store(...) + AdvanceWatermark; SetLastSequenceAtLeast(watermark - 1) (§3.6)
  9. file_writer->ReleaseSlot(slot)                // fetch_sub(1, release)
 10. Return success
```

**After the mutex is released in step 3 the write is committed.** There is no error path, no rollback, no abort. Anything that could fail or wait must happen in step 1 (prewrite). Any IO error in steps 5–9 triggers process abort (§3.11).

WriteThread coordination is fully bypassed — no `WriteThread::JoinBatchGroup`, no leader/follower roles, no `LinkOne` linked list.

### 3.2 Paired (next_lsn, next_wal_offset) advance

The pair-advance happens inside the single `wal_write_mutex_` critical section described in §3.1 step 3. The mutex is an adaptive `InstrumentedMutex` (spins briefly before sleeping), which is cheap for the short IO-free CS here.

#### 3.2.1 Fields

- `next_lsn_`: plain `uint64_t` member on `DBImpl`. Monotonic for the lifetime of the DB. Advanced by writers under `wal_write_mutex_`. Not reset across buffer switches. Seeded from recovery in `DBImpl::Recover`.
- `slot->next_wal_offset`: per-slot `uint64_t` on `BufferSlot`. Each slot has its own. Reset to `0` by `ResetDrainedSlot` after the slot has been drained and flushed; carried into the next active cycle's `file_offset_start` by `SwitchActive`.
- `slot->file_offset_start`: absolute WAL-file offset corresponding to byte 0 of this slot's scratch region. Set by `SwitchActive` before publishing the slot. Needed by `SerializeRecord` for block-boundary padding (§3.10).

#### 3.2.2 Advance protocol (inside wal_write_mutex_)

```
InstrumentedMutexLock l(&wal_write_mutex_);
  slot        = file_writer->ClaimSlot();             // bumps in_flight_count
  start_lsn   = next_lsn_;
  start_offset = slot->next_wal_offset;
  byte_count  = log::ComputeSerializedRecordSize(
                   payload_size,
                   slot->file_offset_start + start_offset,
                   recycle);
  next_lsn_              += batch_lsn_count;
  slot->next_wal_offset += byte_count;
```

Because the mutex serializes ClaimSlot + pair-advance + SwitchActive, writers never race against the switcher. The switcher's publish (`SwitchActive`) only runs when the mutex is held, so `slot` captured by a writer is guaranteed to be the active slot for the entire body of the CS. Once the writer exits the CS with its in-flight count bumped, the switcher's `WaitDrain` (which runs without the mutex) waits for that count to return to zero before any flush touches the slot's bytes.

#### 3.2.3 Partial-order invariant (within a slot)

> For any two writers i and j that both claim in the same slot A:
> `lsn[i] < lsn[j]  ⇔  A_offset[i] < A_offset[j]`

Holds because `wal_write_mutex_` serializes the pair-advance — both fields are advanced together inside one critical section.

#### 3.2.4 Cross-slot lsn monotonicity

Writers in the old slot `A` all finish their lsn advance **before** any writer claims in the new slot `B`. This is enforced by `SwitchActive` running under `wal_write_mutex_`:

1. Switcher takes `wal_write_mutex_`, reads `A = ActiveSlot()`, calls `SwitchActive(A, B)`. That call freezes `A`'s `next_wal_offset`, sets `B->file_offset_start = A->file_offset_start + A->next_wal_offset`, and release-stores `active_buffer_ptr_ = B`.
2. Switcher releases the mutex and calls `WaitDrain(A)` (spin on `A->in_flight_count`, acquire load) outside the mutex.
3. Any writer that held the mutex after the switcher will see `B` as the active slot (ClaimSlot loads `active_buffer_ptr_`).

Therefore: at the moment the first `B`-writer advances `next_lsn_`, every advance of `next_lsn_` by an `A`-writer has already happened. All lsns in `A` are strictly less than all lsns in `B`. The WAL file, when flushed in slot order (`A` then `B`), is monotonic in lsn across the slot boundary. Recovery (§4.4) does not need any special handling for slot transitions.

#### 3.2.5 Why one adaptive mutex covers claim + pair-advance + switch

- Adaptive mutex spins briefly before sleeping — the hot-path CS is a handful of plain loads/stores, so the spin typically absorbs the handoff.
- One mutex serializes `ClaimSlot` + pair-advance + `SwitchActive`, so the switcher never needs to race against in-flight claimers — their in_flight_count is already bumped, and `WaitDrain` is a plain acquire-load spin outside the mutex.
- Portable (no wider atomics required).

### 3.3 WAL buffer (v1: infinite cap)

- The active buffer is **infinite capacity** in v1 — advancing `A.next_wal_offset` (§3.2) never fails on size.
- Buffer memory is bounded by `sync_interval × write_rate × 2` (one active, one being flushed).
- Two slots exist (ping-pong): the **active** slot accepts new writes; the **standby** slot is either idle or being flushed. The active/standby roles flip on each `SwitchActive` call (§3.5).
- The only swap triggers in v1 are:
  1. `SyncWAL` is called (§3.4).
  2. A structural switch fires from prewrite (§3.9.1) — WAL too big, WBM should flush, trim_history non-empty, or flush_scheduler non-empty.
- v2 will add a soft-cap-driven proactive swap (see §4.3 and §4.9).

### 3.4 SyncWAL protocol

*Implementation note (2026-04-15):* live in `db/db_impl/db_impl_sync_wal_no_group.cc :: DBImpl::SyncWALNoGroup`. Dispatched from `SyncWAL()` when `use_group_commit=false`. The on-disk flush uses `NonBlockingIOWritableFileWriter::FlushSlotBytes(A)` — `A.bytes` is the AlignedBuffer aliased as `slot_bytes_[0]` or `slot_bytes_[1]` (§4.4 Case 4). IO failure on the switcher path aborts the process per §3.11 (Phase 3 has already published `B`, so returning an error would leave `A`'s on-disk state undefined).

SyncWAL uses `SwitchActive` + `WaitDrain` to close the active slot, then flushes and fsyncs it. The difference from a structural switch (§3.9.1) is that SyncWAL does **not** run `SwitchWAL` / `SwitchMemtable` / `TrimMemtableHistory` / `ScheduleFlushes`. Concurrent SyncWAL callers serialize at entry via the existing `IsSyncing()` + `log_sync_cv_` machinery (db/db_impl/db_impl.cc:1494, db/db_impl/db_impl.h:2413).

```
SyncWAL():
  InstrumentedMutexLock l(&wal_write_mutex_);
  // Round-2 scope guard; becomes a walk over logs_ once structural switch lands.
  assert(logs_.size() == 1);

  // Loser path: if another switcher is mid-sync, wait for its FinishSync to
  // cover our entry-time durability requirement.  On wake, logs_.back() may
  // have advanced; the Round-3 version will walk logs_ and return if ours
  // is already covered.
  while (logs_.back().IsSyncing())
    log_sync_cv_.Wait();

  log_writer  = logs_.back().writer;
  file_writer = static_cast<NonBlockingIOWritableFileWriter*>(log_writer->file());
  // IsSyncThreadSafe check, capture current_log_number / need_log_dir_sync ...

  A = file_writer->ActiveSlot();
  B = file_writer->OtherSlot(A);
  file_writer->SwitchActive(A, B);              // publishes B, freezes A's offsets
  logs_.back().PrepareForSync();                // IsSyncing = true
  l.Unlock();  // end of CS

  file_writer->WaitDrain(A);                    // spin on A->in_flight_count == 0

  io_s = file_writer->FlushSlotBytes(A);        // write A's bytes to OS
  if io_s.ok():
    io_s = file_writer->SyncWithoutFlush(use_fsync);
  if !io_s.ok():
    std::abort();                               // §3.11 — B is already published

  if need_log_dir_sync:
    dir_s = directories_.GetWalDir()->FsyncWithDirOptions(...);
    if !dir_s.ok(): std::abort();

  InstrumentedMutexLock l2(&wal_write_mutex_);
  MarkLogsSynced(current_log_number, need_log_dir_sync, &synced_wals);
  // (MarkLogsSynced signals log_sync_cv_ and clears IsSyncing.)
  l2.Unlock();

  file_writer->ResetDrainedSlot(A);             // next_wal_offset = 0, ready as standby
  // (done after MarkLogsSynced so waiters wake into a fully-consistent state)

  if synced_wals.IsWalAddition():
    InstrumentedMutexLock ml(&mutex_);
    ApplyWALToManifest(&synced_wals);
  return OK;
```

#### 3.4.1 Key properties

- **No `min_non_hole_lsn` involvement.** SyncWAL only needs `A->in_flight_count == 0` to know `A`'s scratch bytes are fully written. Combined with the partial-order invariant (§3.2.3) and cross-slot monotonicity (§3.2.4), when the count reaches zero every byte position has been written.

- **Durable LSN is well-defined.** LSNs in any one slot form a contiguous range. The reported durable LSN after Sync is the max LSN of that range.

- **No `DBImpl::mutex_` on the fast path.** SyncWAL takes only `wal_write_mutex_` (twice — once for the switch+PrepareForSync, once for MarkLogsSynced). `DBImpl::mutex_` is only touched on the manifest-update tail (`ApplyWALToManifest`), same as today's baseline.

- **Switch publishes B before the flush.** New writers can claim in `B` while the switcher is still appending `A`'s bytes to the WAL file. The flush of `A` and the writes into `B` proceed concurrently.

#### 3.4.2 Concurrent SyncWAL callers collapse

If two threads call SyncWAL concurrently:

1. T1 acquires `wal_write_mutex_`, sees `IsSyncing()==false`, calls `SwitchActive(A,B)` + `PrepareForSync()`, releases the mutex, and proceeds with `WaitDrain` / flush / fsync.
2. T2 acquires `wal_write_mutex_`, sees `IsSyncing()==true`, waits on `log_sync_cv_` inside the CS.
3. T1 finishes the IO, re-takes the mutex for `MarkLogsSynced` (which signals `log_sync_cv_` and clears `IsSyncing`).
4. T2 wakes, re-checks the condition — covered by T1's FinishSync — and returns success.

T2's success guarantees everything T2 could have wanted synced is durable, because T2's own writes must have completed before T2's SyncWAL call, and T1's switch+sync covers the slot they landed in.

**Sub-optimization (deferred):** in this scheme T2 is correct but wasteful — if more than two threads pile up, every one after T2 will do its own switch+sync once it wakes, even though T1's sync already covered them. A free-riding optimization (snapshot a "pending sync covers up to LSN X" marker, let waiters return without their own switch) is straightforward but not yet implemented.

#### 3.4.3 Interaction with structural switch (§3.9.1)

Both SyncWAL and the structural switch path call `SwitchActive` under `wal_write_mutex_`. Because the mutex serializes the call, only one runs at a time. If a SyncWAL caller arrives mid-structural-switch (or vice versa), the second waits on `log_sync_cv_` via the `IsSyncing()` gate at entry — the same wait T2 takes in §3.4.2.

A structural switcher's flush of A also makes A durable (it runs through the same `PrepareForSync → FlushSlotBytes → SyncWithoutFlush → FinishSync` sequence), so a SyncWAL caller losing the race to a structural switch is correctly served. Conversely, SyncWAL does no structural work, so a structural condition that fired in prewrite remains set after SyncWAL returns — the prewrite-triggered writer will re-enter prewrite and run the structural switch on its next pass.

### 3.5 Slot primitives

Each `NonBlockingIOWritableFileWriter` carries two `BufferSlot` objects (the same ping-pong as the legacy nonblocking_wal_io path; in the use_group_commit=false mode `slots_[0]` aliases `buf_` and `slots_[1]` aliases `flush_buf_` — see §4.4 Case 4). One `std::atomic<BufferSlot*> active_buffer_ptr_` names the slot that is currently active.

#### 3.5.1 Per-slot fields

```
struct BufferSlot {
  std::atomic<uint64_t> in_flight_count;  // writers between Claim and Release
  uint64_t              next_wal_offset;  // bytes reserved in this slot so far
  uint64_t              file_offset_start; // absolute WAL-file offset at slot byte 0
};
```

There is no state field. The switcher does not need one: claim and switch are both serialized by `wal_write_mutex_`, and the only operation done outside the mutex (the switcher's drain) only reads `in_flight_count`.

`file_offset_start` is needed by `SerializeRecord` for log-record block-boundary padding (§3.10): the writer's absolute offset is `slot->file_offset_start + start_offset`. It is set by `SwitchActive` before the slot is published.

#### 3.5.2 ClaimSlot — under `wal_write_mutex_`

```
BufferSlot* ClaimSlot():
  s = active_buffer_ptr_.load(relaxed);   // mutex-held — relaxed is enough
  s->in_flight_count.fetch_add(1, relaxed);
  return s;
```

`relaxed` everywhere because `wal_write_mutex_` provides the happens-before edges to and from the switcher's CS. The returned slot is the one the writer's pair-advance and SerializeRecord operate against — the writer does not re-read `active_buffer_ptr_` later.

#### 3.5.3 ReleaseSlot — lockless

```
ReleaseSlot(BufferSlot* slot):
  slot->in_flight_count.fetch_sub(1, release);
```

Release ordering pairs with `WaitDrain`'s acquire load: the writer's stores into the slot's scratch bytes (and its `AdvanceFileSize` bump) happen-before the switcher observes count == 0. Called after memtable insert, SerializeRecord, AdvanceFileSize, and Link_buf advance (§3.6) all complete — i.e., as the last step of the writer's commit.

#### 3.5.4 SwitchActive — under `wal_write_mutex_`

```
SwitchActive(BufferSlot* A, BufferSlot* B):
  // Caller proves A is the active slot (it just read it under the mutex).
  // B is the standby — its in_flight_count is 0 and its next_wal_offset is 0
  // (zeroed by the previous cycle's ResetDrainedSlot).
  B->file_offset_start = A->file_offset_start + A->next_wal_offset;
  active_buffer_ptr_.store(B, release);          // publish B as active
  // A's next_wal_offset is now frozen — no new ClaimSlot can land in A,
  // and existing claimers' counts will drain.
```

A subsequent `ClaimSlot` (which loads `active_buffer_ptr_` under the same mutex) lands in B. Existing in-flight A-writers are unaffected — they keep advancing A's bytes; the switcher waits them out via `WaitDrain`.

#### 3.5.5 WaitDrain — outside `wal_write_mutex_`

```
WaitDrain(BufferSlot* slot):
  while slot->in_flight_count.load(acquire) != 0:
    port::AsmVolatilePause();
```

Acquire load pairs with each `ReleaseSlot`'s release decrement, so once the spin observes zero, every writer's bytes and AdvanceFileSize bump are visible. Run outside the mutex so writers can take it for their own ReleaseSlot (releases are lockless, but other claims may queue behind the switcher).

#### 3.5.6 ResetDrainedSlot

```
ResetDrainedSlot(BufferSlot* slot):
  // Caller has finished WaitDrain + flush + sync; slot is privately owned.
  slot->next_wal_offset = 0;  // in_flight_count is already 0
```

After this call the slot is the next standby; the next `SwitchActive` will pass it as `B` and reset its `file_offset_start`. Plain store — no synchronization needed because the slot has no observers until the next switcher publishes it under the mutex.

#### 3.5.7 Why a single mutex is enough

Writers already serialize on `wal_write_mutex_` to advance `(next_lsn_, next_wal_offset)` as a pair (§3.2.3 partial-order invariant), so adding ClaimSlot and SwitchActive to the same CS costs O(1) atomic operations on top of work that has to happen anyway. The slot needs no state field; the switcher needs no CAS to claim ownership. The only operation outside the mutex is the switcher's drain, which is a plain acquire-load spin paired with `ReleaseSlot`'s release decrement.

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

The link store (§3.6.2) and watermark advance (§3.6.3) happen at step 8 of the §3.1 pipeline, after memtable insert, SerializeRecord, and AdvanceFileSize, and before `ReleaseSlot` (step 9). This ordering ensures that when `min_non_hole_lsn` advances past a batch's LSN range, the batch's memtable entries and WAL bytes are fully written.

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

The structural switch reuses the same `SwitchActive` + `WaitDrain` primitives as SyncWAL, with the structural rewire interposed between the publish and the flush. Sequencing:

```
structural_switch_path():
  InstrumentedMutexLock l(&wal_write_mutex_);
  // Loser at entry: another switcher (SyncWAL or structural) holds the log mid-sync.
  while (logs_.back().IsSyncing())
    log_sync_cv_.Wait();

  log_writer  = logs_.back().writer;
  file_writer = NonBlockingIOWritableFileWriter cast of log_writer->file();
  A = file_writer->ActiveSlot();
  B = file_writer->OtherSlot(A);

  // ----- Structural work, under DBImpl::mutex_, BEFORE we publish B -----
  // (Round 3 work — wired this way so B-writers only commit against the new
  // memtable + new WAL.)
  acquire DBImpl::mutex_
    if WAL too big:            SwitchWAL(...)
    if WBM should flush:       HandleWriteBufferManagerFlush(...)
    if !trim_history.Empty():  TrimMemtableHistory(...)
    if !flush_sched.Empty():   ScheduleFlushes(...)
  release DBImpl::mutex_

  // After SwitchWAL, logs_.back() is the NEW writer — but A still belongs to
  // the OLD writer.  Capture A's log pointer here so the async flush below
  // targets the right file.
  log_for_A = log_writer;
  file_writer->SwitchActive(A, B);              // publishes B
  logs_for(A).PrepareForSync();                 // marks A's log syncing
  l.Unlock();

  // ----- Async flush of A, targeting the OLD log -----
  // Same sequence as SyncWAL §3.4:
  //   WaitDrain(A), FlushSlotBytes(A), SyncWithoutFlush, dir fsync if needed,
  //   {wal_write_mutex_} MarkLogsSynced + cv signal, ResetDrainedSlot(A).
  // Run on a background task so the triggering writer can re-enter prewrite.

  return RETRY_PREWRITE
```

**Sequencing rationale.** The structural rewire must run before `SwitchActive` (so B-writers commit against the new memtable / new WAL), but the entry-time `IsSyncing()` wait already serializes us against any in-flight A-flush. Because all of (read A, structural rewire, SwitchActive) happen under `wal_write_mutex_`, no writer can land in B before B is published, and no writer can race the structural handlers.

**Async flush of A targets the OLD WAL file.** A's bytes were accumulated against the old log; the flush must append there. Since we capture `log_for_A` before SwitchWAL re-points `logs_.back()`, the async task uses the right writer.

**Coalescing.** Multiple structural handlers share one entry, one drain, and one slot roll. Triggered conditions handled in one pass.

**Multiple writers racing to switch.** Serialized by `wal_write_mutex_` and the entry-time `IsSyncing()` wait — only one runs the structural work. Loss is correct: the loser re-enters prewrite, sees its trigger resolved (or not), and proceeds.

**Spin window for new writers.** Adaptive `wal_write_mutex_` blocks new claims during the structural CS. They take the mutex once it is released and land in B. The new writers do not see any in-between state: there is none.

**Note on `WaitForPendingWrites`.** The existing function `DBImpl::WaitForPendingWrites` is a **literal no-op** when `unordered_write=false && enable_pipelined_write=false` (verified at db/db_impl/db_impl.h:1926). The drain in `WaitDrain` is a **new** mechanism, not a replacement of `WaitForPendingWrites`. The new drain is needed because the new path **removes the WriteThread leader/group serialization** that previously made no-drain safe.

#### 3.9.2 Stall / wait path (no drain)

Triggered by: `write_controller_.IsStopped()`, `NeedsDelay()`, `WriteBufferManager::ShouldStall()`. These are pure waits — they do not structurally mutate memtable/WAL, so in-flight writers can keep landing in the active buffer while the stalled writer sleeps.

```
stall_path():
  acquire DBImpl::mutex_
    wait on stall cv until condition clears (DelayWrite / WriteBufferManagerStallWrites equivalent)
  release DBImpl::mutex_
  // writer goes back to prewrite and retries
```

**No coordination with the switch path.** A stalled writer holds `mutex_` only while sleeping on the cv and does not block independent writers on the fast path — the fast path takes only `wal_write_mutex_` (briefly).

**If both a stall condition and a switch condition fire in the same prewrite pass:** handle the switch path first (it advances WAL/memtable state and may clear the stall), then retry prewrite. Stall path runs only when prewrite sees a stall *and no switch condition*.

**Note on `WaitForPendingWrites`:** the existing function `DBImpl::WaitForPendingWrites` is a **literal no-op** when `unordered_write=false && enable_pipelined_write=false` (verified at db/db_impl/db_impl.h:1926). The drain in the switch path is a **new** mechanism, not a replacement of `WaitForPendingWrites`. The reason it is needed is that the new path **removes the WriteThread leader/group serialization** that previously made no-drain safe.

### 3.10 Memtable switch and WAL switch are coupled

- A memtable switch always rolls a new WAL (matches current behavior with `two_write_queues=false`).
- A WAL switch always rolls a new (fresh, empty) buffer.
- One switch operation, one drain, one new buffer.

### 3.11 Error policy: abort on any IO error

- Any IO error encountered after `ClaimSlot` (steps 4–9 of the §3.1 pipeline: ring guard, memtable insert, SerializeRecord, AdvanceFileSize, Link_buf completion, ReleaseSlot) → process abort. Once the writer leaves the §3.1 step-3 critical section the write is committed; there is no rollback path.
- Any IO error in `SyncWAL`'s flush (§3.4) → process abort.
- Any IO error in the structural switch path's async flush of A (§3.9.1) → process abort.
- This eliminates the "user was told success but later sync failed" semantic regression that the asynchronous flush model would otherwise create.
- Consistent with the existing nonblocking_wal_io durability model, which already accepts ~100ms data loss on process crash.

---

## 4. Open items and resolutions

Items 4.1, 4.2, 4.4 (all cases), 4.5, 4.6, 4.7, 4.8 are resolved. Items 4.3 and 4.9 are deferred to v2. Item 4.10 is the remaining work — the component breakdown for the implementation pass.

### 4.1 Stall handling — RESOLVED

**Decision:** stalls stay in a **separate wait zone** from the structural switch path. Stall path acquires `mutex_`, sleeps on the stall cv, and retries prewrite — no in-flight drain, no interaction with the switch path. `TrimMemtableHistory` is treated as a structural handler (it mutates memtable history) and runs under the switch path, not the stall path.

Detailed in §3.9.1 (switch path, with drain) and §3.9.2 (stall path, no drain). Open sub-question on ordering when both stall and switch conditions fire in the same prewrite pass: resolved as "handle switch first, then retry prewrite."

**Status:** resolved 2026-04-10.

### 4.2 Claim/switch race — RESOLVED

**Resolution.** Claim and switch are both serialized by `wal_write_mutex_` (adaptive `InstrumentedMutex`); no separate flag, no CAS race. The slot's only field is `std::atomic<uint64_t> in_flight_count`. See §3.5 for the protocol and §3.5.7 for why one mutex is enough.

**Status:** resolved 2026-04-15.

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

**Contract the modification must preserve.** In the default mode (`nonblocking_wal_io=true` but `use_group_commit=true`, still going through the WriteThread leader), `NonBlockingIOWritableFileWriter` must behave exactly as today. The slot fields default to benign values (in_flight_count=0, next_wal_offset=0, file_offset_start=0) that the legacy ping-pong path simply does not read. The constructor's `use_group_commit` parameter selects between the legacy small-flush_buf preallocation and the slot-aliased pre-grown buffers. Tests for `nonblocking_wal_io` on master must continue to pass unchanged.

**Status:** resolved 2026-04-14 — modify in place, revisit after stabilization.

#### Case 1 — SyncWAL + structural switch race on the same buffer — RESOLVED (conditional on Case 4 decision)

SyncWAL (§3.4) and structural switch (§3.9.1) both call `SwitchActive` under `wal_write_mutex_`. Mutex serialization makes the slot-level race trivial. The log::Writer-level interaction reuses the existing `IsSyncing()` + `log_sync_cv_` machinery:

- Both paths drive `PrepareForSync()` / `FlushSlotBytes()` / `SyncWithoutFlush()` / `FinishSync()` on the underlying `NonBlockingIOWritableFileWriter`. Only one can hold `IsSyncing==true` at a time; concurrent callers queue via `log_sync_cv_` at entry. This is the same serialization the `nonblocking_wal_io` baseline already uses for concurrent `SyncWAL` callers (db/db_write_test.cc:859 exercises this).
- A SyncWAL caller that arrives mid-flush waits on `log_sync_cv_` inside the `wal_write_mutex_` CS until `IsSyncing()` clears on `logs_.back()` — covered by the in-progress flush.
- Across a log::Writer boundary (a structural switcher did `SwitchWAL`, the new log is now current), a SyncWAL caller that wanted bytes from the **old** log must walk `logs_` the way today's SyncWAL does (db/db_impl/db_impl.cc:1491-1506 iterates all logs with `number <= current_log_number`). §3.4's new protocol must adopt this iteration rather than assuming a single-log shortcut.

**Status:** resolved 2026-04-15. `DBImpl::SyncWALNoGroup` waits on `log_sync_cv_` inside the `wal_write_mutex_` CS until `IsSyncing()` clears on `logs_.back()`. Iteration over multiple logs is deferred to Round 3; the scope guard `assert(logs_.size() == 1)` holds until the structural-switch path lands.

### 4.5 WriteBatch validation timing — RESOLVED

**Where validation lives today.** Two places on the leader's side of the group-commit path:

1. `DBImpl::MergeBatch` (db/db_impl/db_impl_write.cc:1224) walks the write group and calls `WriteBatchInternal::Append(*merged_batch, writer->batch, /*WAL_only*/true)` for each follower. `Append` implicitly validates the batch (parses the header, rejects malformed batches) and the first failure fails the whole group.
2. `DBImpl::WriteToWAL` (db/db_impl/db_impl_write.cc:1280) calls `merged_batch.VerifyChecksum()` once on the merged batch, right before `log_writer->AddRecord`.

Already per-thread and unaffected: `WriteCallback` (pre-merge), and `protection_bytes_per_key` per-KV verification at memtable insert time.

**Where validation moves in the new path.** `MergeBatch` disappears entirely — no merging, each writer writes its own bytes. Both validations collapse into per-writer prewrite, **before `ClaimSlot` (§3.5):**

1. `batch->VerifyChecksum()` on the writer's own batch. This is the same call today's `WriteToWAL` makes on the merged batch, just moved to prewrite and scoped to one batch.
2. Batch format sanity — equivalent to what `WriteBatchInternal::Append` would have caught. A dedicated helper (e.g., `WriteBatchInternal::CheckContents(batch)` that parses the header and walks records without copying) is the cleanest option; at minimum a debug-only `Append` to a scratch buffer catches regressions.

Both are lockless, per-thread atomic/concurrent reads — they fit §3.8 trivially. They run **before** `ClaimSlot` because that is the point of no return: once a writer owns an in-flight slot, the design has no rollback (§3.11). A validation failure in prewrite is returned to the caller as a regular `Status` without ever touching slot state.

**What the implementation must guarantee.** No side effects before `ClaimSlot`: validation must not advance `next_lsn_`, must not touch any slot's `in_flight_count` or scratch bytes, must not write to the Link_buf ring. On failure, the writer returns the status and the caller sees exactly what it sees today on a validation failure.

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

- **No deadlock.** A stalled writer holds `mutex_` only while sleeping on the cv. It must not hold any buffer slot (it has not yet run `ClaimSlot` — that happens after prewrite), must not hold `wal_write_mutex_`, and must not block a switcher's drain. Fast-path writers already past their §3.1 step-3 CS must be able to complete and release their slots while other writers are stalled.
- **No crash.** The stall path must not touch buffer state, `next_lsn`, `next_wal_offset`, or the Link_buf ring. It is a pure wait; on wakeup the writer re-enters prewrite from the top and re-claims a slot normally.
- **Stall + switch interaction.** Per §3.9.2, if a stall condition and a switch condition fire in the same prewrite pass, the switch path runs first. A writer that sees only a stall condition waits on the cv; when the stall clears (via a flush completion or WriteController state change), it retries prewrite — possibly discovering a switch condition this time, which it then handles through §3.9.1.

**Unit tests required:**

1. **Stall does not deadlock the fast path.** Put N writers into the stall path (e.g., force `WriteBufferManager::ShouldStall()` to return true), while M other writers are on the fast path. The fast-path writers must complete. The stalled writers must wake and complete after the stall condition clears.
2. **Stall + concurrent switcher.** Trigger a structural switch (WAL rotate) while writers are stalled. The switcher must be able to complete SwitchActive + WaitDrain + flush of `A` without needing the stalled writers to wake.
3. **Stall + concurrent SyncWAL.** A SyncWAL caller must complete (SwitchActive + WaitDrain + flush) while stalled writers are asleep on the cv. Stalled writers never held a slot, so drain is trivially satisfied; the test pins this assumption.
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
