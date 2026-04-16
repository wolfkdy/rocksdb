# Eliminate Group Commit — Design Notes

**Status:** Describes the current in-tree design. Code lives on branch `work-from-7.7.fb` on top of the `nonblocking_wal_io` feature.
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

*Implementation:* `db/db_impl/db_impl_write_no_group.cc :: DBImpl::WriteImplNoGroup`, dispatched from `WriteImpl()` when `use_group_commit=false`. Reader visibility is published via `VersionSet::SetLastSequenceAtLeast(Link_buf::AdvanceWatermark() - 1)` — a CAS-max loop driven by the monotonic watermark return (§3.6), not by `SetLastSequence` (which asserts monotone input that out-of-order writer completion would violate).

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

*Implementation:* `db/db_impl/db_impl_sync_wal_no_group.cc :: DBImpl::SyncWALNoGroup`, dispatched from `SyncWAL()` when `use_group_commit=false`. On-disk flush of the back log's active slot uses `NonBlockingIOWritableFileWriter::FlushSlotBytes(A)` (§4.4 Case 4). IO failure on any path aborts per §3.11.

SyncWAL is the **sole fsync authority** for the leaderless path. Structural switches (§3.9.1) push bytes into the OS via `FlushSlotBytes` but do **not** fsync — matching legacy `SwitchMemtable`'s `WriteBuffer` semantics (`PosixWritableFile::Flush` is a no-op; `Fsync`/`Fdatasync` only happens on the SyncWAL path). Consequently, when `SyncWAL` runs and `logs_` contains inactive entries from recent structural switches, it walks the whole deque and fsyncs each entry, exactly like legacy `DBImpl::SyncWAL` (db_impl.cc:1491-1506).

Concurrent SyncWAL callers serialize at entry via `IsSyncing()` + `log_sync_cv_`.

```
SyncWAL():
  InstrumentedMutexLock l(&wal_write_mutex_);

  // Loser path: wait until NO log in logs_ is mid-sync.  A prior
  // SyncWAL may be fsyncing a subset of logs right now — we can't
  // start a new cycle until it clears IsSyncing via MarkLogsSynced.
  while (some log in logs_ has IsSyncing == true)
    log_sync_cv_.Wait();

  // Capture the back log + its active slot (writable).
  back = logs_.back();
  back_fw = NonBlockingIOWritableFileWriter of back.writer->file();
  current_log_number = back.number;
  need_log_dir_sync = !log_dir_synced_;
  A = back_fw->ActiveSlot();
  B = back_fw->OtherSlot(A);
  back_fw->SwitchActive(A, B);                  // freeze A on back_fw; new writers land in B
  back.PrepareForSync();                        // IsSyncing = true

  // For every INACTIVE entry (all of logs_ except back):
  //   PrepareForSync.  Its bytes are already in the file (structural
  //   switch did FlushSlotBytes under wal_write_mutex_ when it rotated
  //   the WAL); we need only to fsync outside the CS.
  //   Collect file_writer pointers for the post-CS loop; pointers stay
  //   valid because IsSyncing=true prevents MarkLogsSynced erasures.
  inactive_fws = []
  for each log in logs_ except back:
    log.PrepareForSync()
    inactive_fws.append(log.writer->file())

  l.Unlock();

  // Back log: drain A (memtable-swap barrier is not at stake here, but
  // we still need A's bytes stable before FlushSlotBytes), then push
  // the bytes to OS, then fsync.
  back_fw->WaitDrain(A)
  io_s = back_fw->FlushSlotBytes(A)
  if io_s.ok(): io_s = back_fw->SyncWithoutFlush(use_fsync)
  if !io_s.ok(): std::abort()

  // Inactive logs: bytes are already in OS cache; just fsync.
  for fw in inactive_fws:
    io_s = fw->SyncWithoutFlush(use_fsync)
    if !io_s.ok(): std::abort()

  if need_log_dir_sync:
    dir_s = directories_.GetWalDir()->FsyncWithDirOptions(...)
    if !dir_s.ok(): std::abort()

  InstrumentedMutexLock l2(&wal_write_mutex_);
  // MarkLogsSynced walks logs_ up_to current_log_number.  For each
  // inactive log, pre_sync_size == GetFlushedSize (no bytes added
  // between PrepareForSync and here), so it takes the "fully synced"
  // branch and erases the entry.  For the back log, pre_sync_size <
  // GetFlushedSize (FlushSlotBytes added A's bytes), so MarkLogsSynced
  // takes the "Active WAL" branch and just FinishSyncs.
  MarkLogsSynced(current_log_number, need_log_dir_sync, &synced_wals);
  l2.Unlock();

  back_fw->ResetDrainedSlot(A)                  // recycle A as next standby

  if synced_wals.IsWalAddition():
    InstrumentedMutexLock ml(&mutex_);
    ApplyWALToManifest(&synced_wals);
  return OK;
```

#### 3.4.1 Key properties

- **No `min_non_hole_lsn` involvement.** SyncWAL only needs `A->in_flight_count == 0` to know `A`'s scratch bytes are fully written. Combined with the partial-order invariant (§3.2.3) and cross-slot monotonicity (§3.2.4), when the count reaches zero every byte position has been written.

- **Durable LSN is well-defined.** LSNs in any one slot form a contiguous range. The reported durable LSN after Sync is the max LSN of that range.

- **No `DBImpl::mutex_` on the fast path.** SyncWAL takes only `wal_write_mutex_` (twice — once for the switch+PrepareForSync, once for MarkLogsSynced). `DBImpl::mutex_` is only touched on the manifest-update tail (`ApplyWALToManifest`), same as today's baseline.

- **Switch publishes B before the flush.** On the back log, new writers can claim in `B` while the switcher is still appending `A`'s bytes to the WAL file. The flush of `A` and the writes into `B` proceed concurrently.

- **Multi-log sync is fsync-only.** Inactive logs never need `FlushSlotBytes` in SyncWAL (structural switch already did it); they need only `SyncWithoutFlush`. This keeps the per-log cost of many-inactive-logs cases bounded to N fsyncs.

#### 3.4.2 Concurrent SyncWAL callers collapse

If two threads call SyncWAL concurrently:

1. T1 acquires `wal_write_mutex_`, sees no log IsSyncing, calls `SwitchActive(A,B)` + `PrepareForSync()` on back and on every inactive log, releases the mutex, and proceeds with drain + flush + fsync.
2. T2 acquires `wal_write_mutex_`, sees some log IsSyncing, waits on `log_sync_cv_` inside the CS.
3. T1 finishes the IO, re-takes the mutex for `MarkLogsSynced` (which signals `log_sync_cv_` and clears IsSyncing on every entry up to current_log_number).
4. T2 wakes, re-checks the condition — covered by T1's FinishSync — and returns success after running through its own (now mostly trivial) cycle.

T2's success guarantees everything T2 could have wanted synced is durable, because T2's own writes must have completed before T2's SyncWAL call, and T1's sync covered their log(s).

**Sub-optimization (deferred):** T2's "mostly trivial" cycle still does a SwitchActive on the back log and one fsync of its own bytes. If many callers pile up, they serialize through that cycle even when T1's work covered them. A free-riding optimization (snapshot a "pending sync covers up to LSN X" marker, let waiters return without their own switch) is straightforward but not yet implemented.

#### 3.4.3 Interaction with structural switch (§3.9.1)

Structural switch takes `wal_write_mutex_` inside `SwitchMemtable` to run `WaitDrain + FlushSlotBytes + emplace_back(new_log)`. It does NOT call `PrepareForSync` on the old log — the old log stays in `logs_` with IsSyncing=false, bytes in OS cache but not fsynced.

SyncWAL picks up those inactive entries on its next run: they appear in `logs_` at indices 0..size-2, SyncWAL calls PrepareForSync + SyncWithoutFlush on each, and MarkLogsSynced erases them on the way out.

Serialization between the two paths is via `wal_write_mutex_` for the brief CSs (claims, SwitchActive, emplace_back) and via `IsSyncing` + `log_sync_cv_` for the longer-running drain/flush phase of SyncWAL. A SyncWAL caller mid-fsync blocks a concurrent structural switcher's `nonblocking_wal_io_` `IsSyncing` wait in `SwitchMemtable`; the structural switcher proceeds once MarkLogsSynced signals.

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

| Check | Test | Lockless? | Live trigger? |
|---|---|---|---|
| DB stopped | `error_handler_.IsDBStopped()` | yes | yes |
| WAL too big | `total_log_size_ > GetMaxTotalWalSize()` | yes | **NO** — see note |
| WBM should flush | `write_buffer_manager_->ShouldFlush()` | yes | yes |
| TrimHistory pending | `!trim_history_scheduler_.Empty()` | yes (concurrent queue) | yes |
| Pending flushes | `!flush_scheduler_.Empty()` | yes (concurrent queue) | yes |
| Stall / delay | `write_controller_.IsStopped() \|\| NeedsDelay()` | yes | yes (§3.9.2) |
| WBM should stall | `write_buffer_manager_->ShouldStall()` | yes | yes (§3.9.2) |
| `need_log_sync` | local field | yes | unused (§3.8 note below) |

**`wal_too_big` is disabled as a live trigger.** `PreprocessWriteLockless` hard-wires `cond.wal_too_big = false`. `total_log_size_` does not drop when `SwitchWAL` runs (only when the old WAL is deleted by background file-cleanup), so making it a retry trigger would livelock and making it a one-shot trigger would diverge from the self-resolving model of the other three structural reasons. It is handled by opportunistic piggy-backing inside `StructuralSwitchNoGroup` when the handler section is entered for another reason. The edge case where `wal_too_big` is the *only* active signal is a known deferred gap — see §4.11.

**`need_log_sync` is unused.** `WriteImplNoGroup` rejects `WriteOptions::sync=true` and always passes `need_log_sync=false` to `PreprocessWriteLockless`. Durability is the caller's responsibility, via `DBImpl::SyncWAL()`.

Additionally, prewrite rejects batches that exceed the Link_buf ring capacity (§3.6.4 Check 1) and corrupt batches (§4.5):

| Check | Test | Lockless? |
|---|---|---|
| Batch too large for Link_buf | `batch_count >= kCapacity` | yes |
| Batch `VerifyChecksum` failed | per-batch checksum walk | yes |

When **none** of the conditions fire (the common case), prewrite is a sequence of atomic reads with no locks. The writer falls through directly to §3.2.

**Prewrite must run strictly before the (lsn, offset) claim.** Anything that could fail or wait must happen here, because after the claim there is no rollback path.

### 3.9 Slow-path serializer

Prewrite failures split into **two disjoint slow paths** depending on what the failing check needs. Structural switches need a drain; stalls/waits do not. They never share state.

#### 3.9.1 Structural switch path (drain required)

*Implementation:* `db/db_impl/db_impl_write_no_group.cc :: DBImpl::StructuralSwitchNoGroup`, plus a leaderless branch in `DBImpl::SwitchMemtable` (`db/db_impl/db_impl_write.cc`). `SwitchMemtable` does `WaitDrain + FlushSlotBytes` under `wal_write_mutex_` inline — **no fsync** — matching legacy `SwitchMemtable`'s `WriteBuffer` semantics (`PosixWritableFile::Flush` is a no-op; `Fsync`/`Fdatasync` only happens on the SyncWAL path). Durability is deferred to the next `SyncWAL`, which walks `logs_` and fsyncs every covered entry (§3.4).

**Triggers.** Only three structural reasons reach this path:

- `kWbmShouldFlush` — `WriteBufferManager::ShouldFlush()` returned true.
- `kTrimHistoryPending` — `!trim_history_scheduler_.Empty()`.
- `kFlushPending` — `!flush_scheduler_.Empty()`.

**`kWalTooBig` is NOT a prewrite trigger.** `PreprocessWriteLockless` hard-wires `cond.wal_too_big = false` (see `db/preprocess_write_lockless.cc`). Rationale:

- `total_log_size_` does NOT drop when `SwitchWAL` runs — `SwitchWAL` only marks the oldest log `getting_flushed=true`. The counter drops when the old WAL file is deleted by `FindObsoleteFiles` / `PurgeObsoleteFiles`, 10s of ms to seconds later.
- Returning `kWalTooBig` and retrying prewrite would livelock: every retry sees the same condition.
- Making `kWalTooBig` one-shot while the others retry is a divergent special case.

**Instead**, the structural handler piggy-backs a `SwitchWAL` call whenever it is entered for another reason and `total_log_size_ > GetMaxTotalWalSize()`. Repeat entries coalesce cheaply: `SwitchWAL` early-returns once `getting_flushed=true`. The edge case it cannot catch is a DB whose ONLY slow-path signal is `wal_too_big` — see §4.11 for the deferred cleanup.

##### Sequencing

`StructuralSwitchNoGroup` carries no I/O of its own — it is the coalescing + handler-dispatch wrapper. All leaderless WAL work (drain + flush) happens inline inside `SwitchMemtable` under `wal_write_mutex_`.

```
StructuralSwitchNoGroup():                           // no lock held at entry
  // ---- Entry: winner / loser decision (flag + cv under mutex_) ----
  acquire mutex_
    if (structural_switch_in_progress_):
      while (structural_switch_in_progress_):
        structural_switch_cv_.Wait()                 // releases mutex_ while sleeping
      release mutex_
      return OK                                      // loser: caller retries prewrite
    structural_switch_in_progress_ = true
  release mutex_

  // RAII: StructuralSwitchFlagGuard destructor re-takes mutex_, clears
  // the flag, and structural_switch_cv_.SignalAll() on scope exit.

  // ---- Winner: run handlers under mutex_ (single CS) ----
  acquire mutex_
    if error_handler_.IsDBStopped():  status = GetBGError()
    // Piggy-backed wal_too_big check (§3.9.1 header above).
    if status.ok() and total_log_size_ > GetMaxTotalWalSize() and num_cfs > 1:
      status = SwitchWAL(&write_context)             // early-returns if already flushing
    if status.ok() and WBM.ShouldFlush():
      status = HandleWriteBufferManagerFlush(&write_context)
    if status.ok() and !trim_history_scheduler_.Empty():
      status = TrimMemtableHistory(&write_context)
    if status.ok() and !flush_scheduler_.Empty():
      status = ScheduleFlushes(&write_context)
  release mutex_

  return status                                      // guard destructor clears flag
```

There is NO outside-the-lock flush, no `MarkLogsSynced`, no `ResetDrainedSlot` in `StructuralSwitchNoGroup`. Everything WAL-related happens inside `SwitchMemtable`.

##### SwitchMemtable modifications (leaderless branch)

Legacy `SwitchMemtable`'s `wal_write_mutex_` section calls `cur_log_writer->WriteBuffer()`, which bottoms out at `WritableFileWriter::WriteBuffered` (the `write()` syscall) followed by `PosixWritableFile::Flush` (no-op). **No fsync.** Durability is deferred to `SyncWAL`.

The leaderless branch mirrors that contract with slot-aware primitives:

```cpp
// Inside SwitchMemtable's `if (s.ok() && creating_new_log)` wal_write_mutex_ section:
if (!use_group_commit_) {
  // Unchanged from the existing nonblocking_wal_io_ guard:
  while (logs_.back().IsSyncing()) log_sync_cv_.Wait();

  auto* old_fw = static_cast<NonBlockingIOWritableFileWriter*>(
      logs_.back().writer->file());
  auto* A = old_fw->ActiveSlot();
  old_fw->WaitDrain(A);                  // (1) memtable-swap barrier — §3.9.1a
  io_s = old_fw->FlushSlotBytes(A);      // (2) leaderless equivalent of WriteBuffer
  if (!io_s.ok()) std::abort();          // §3.11
} else {
  cur_log_writer->WriteBuffer();         // legacy path, unchanged
}

// After the branch, unchanged for both paths:
logs_.emplace_back(new_log_number, new_log);
log_empty_ = true; log_dir_synced_ = false;
alive_log_files_.push_back(...);
```

Key points:

- **`WaitDrain(A)` is the only genuinely new step** vs. legacy. It exists for the memtable-swap barrier (§3.9.1a) — legacy doesn't need this because the leader's WriteThread ownership already serializes inserts vs. `SwitchMemtable`. Running `WaitDrain` under `wal_write_mutex_` is deadlock-free: in-flight writers' `InsertInto` / `SerializeRecord` / `AdvanceFileSize` / `ReleaseSlot` are all lockless.

- **`FlushSlotBytes(A)` replaces `WriteBuffer()`.** `WriteBuffer()` can't work in the leaderless path:
  - It reads `buf_.CurrentSize()` — always 0 here because leaderless writers use raw memcpy via `SlotBytesStart(slot) + offset`, never `Append`.
  - It only touches `buf_` — wrong when the active slot is slot 1, which aliases `flush_buf_`.

  `FlushSlotBytes(slot)` reads the length from `slot->next_wal_offset` and picks `buf_` or `flush_buf_` based on the slot pointer. Same underlying `WriteToFile` as legacy; different length source.

- **No `SwitchActive` on the old file_writer.** `logs_.emplace_back(new_log)` below makes the old file_writer unreachable to future writers (they read `logs_.back().writer->file()` — the new file_writer — under `wal_write_mutex_`). Redirecting `active_buffer_ptr_` on the old file_writer would be dead work.

- **No `PrepareForSync`, no `MarkLogsSynced`, no fsync.** The old log keeps `IsSyncing=false` and sits in `logs_` as an inactive entry with its bytes in OS cache. `SyncWALNoGroup` (§3.4) walks `logs_` on its next run and fsyncs every inactive entry alongside the back log — same as legacy.

- **All of this runs under `wal_write_mutex_`.** New writers are briefly blocked during WaitDrain + FlushSlotBytes, matching the legacy behavior of holding the lock across `WriteBuffer()`. No async orchestration, no cross-function `WriteContext` handoff.

##### 3.9.1a Memtable-swap correctness: why WaitDrain must run inside SwitchMemtable

Without an explicit drain, this race is possible:

1. Writer W claims on slot A under `wal_write_mutex_`, allocates LSN N, releases the mutex.
2. Structural switcher enters, runs `SwitchMemtable`. In the leaderless path, inside the `wal_write_mutex_` section it calls WaitDrain + FlushSlotBytes + `emplace_back(new_log)`, then releases the mutex and proceeds to the memtable swap (`cfd->imm()->Add(cfd->mem(), ...)`, `cfd->SetMemtable(new_mem)`).
3. (Hypothetical — what would happen *without* WaitDrain in step 2): W has not yet called `InsertInto`. `ColumnFamilyMemTablesImpl::Seek` re-reads `cfd->mem()` — after step 2's swap, this is the NEW memtable. W's entry ends up in the OLD WAL (SerializeRecord writes to slot A's bytes) AND the NEW memtable.

The failure mode: once the OLD memtable flushes to SST, `OldestLogToKeep()` no longer pins the OLD WAL — it gets deleted. If we crash before the NEW memtable flushes, W's entry is lost (its only copies were in the deleted OLD WAL and the unflushed NEW memtable).

The fix is a hard barrier between "writers claim on A" and "memtable swap": the switcher drains A (via `WaitDrain`, acquire-load spin paired with `ReleaseSlot`'s release-decrement) immediately when it enters the `wal_write_mutex_` section, and before `FlushSlotBytes` (so the flush sees a stable `A->next_wal_offset`). `WaitDrain` returning zero proves every A-writer has run `InsertInto` *on the OLD memtable* (InsertInto precedes ReleaseSlot). Only after the drain does `SwitchMemtable` proceed to the flush, release the mutex, and do the memtable swap.

`WaitDrain` runs under `wal_write_mutex_`, the only lock held at that point. In-flight A-writers' `InsertInto`, `SerializeRecord`, `AdvanceFileSize`, and Link_buf operations are all lockless, so the drain does not deadlock. New writers that want to claim on the old file_writer will be briefly blocked by our `wal_write_mutex_`; after we release, they see `logs_.back()` == the new log and claim on the new file_writer instead. The spin is typically microseconds.

##### 3.9.1b Coalescing: the `structural_switch_in_progress_` flag

Concurrent switchers cannot be serialized via `wal_write_mutex_` alone. `SwitchMemtable` releases `mutex_` to call `CreateWAL` (file I/O) and re-acquires afterward. If a second writer enters `StructuralSwitchNoGroup` during that window, it captures `creating_new_log = !log_empty_` *before* the first writer updates `log_empty_ = true` inside the `wal_write_mutex_` section it has not yet reached. Both writers then allocate fresh log numbers and create new WAL files concurrently — the result is an orphan WAL file (correct on disk, but wasteful).

The fix is a DB-level flag `structural_switch_in_progress_` + cv `structural_switch_cv_`, both associated with `mutex_`. The winner holds the flag across the handler-section CS; losers `Wait()` on the cv and return OK on wake, letting the caller retry prewrite. `StructuralSwitchFlagGuard` (nested private class of `DBImpl`, defined in `db_impl_write_no_group.cc`) clears the flag and `SignalAll`s on scope exit — covers every non-abort return path. Abort paths (`std::abort()` on IO failure per §3.11) skip the destructor; a crashing process has no waiters worth signaling.

The flag only needs to cover the handler-section CS — the flush happens inline inside `SwitchMemtable`, so there is no post-handler phase for the flag to guard.

##### 3.9.1c Retry vs one-shot

The outer `WriteImplNoGroup` loop treats structural reasons as **retry** (`continue`) rather than one-shot (`break`). Safe because all three remaining triggers (`kWbmShouldFlush`, `kTrimHistoryPending`, `kFlushPending`) are self-resolving after one switch:

- `ShouldFlush()` flips to false as soon as `MemTable::MarkImmutable()` → `AllocTracker::DoneAllocating()` → `WriteBufferManager::ScheduleFreeMem()` drops `memory_active_` below `mutable_limit_` (happens inside `SwitchMemtable` regardless of whether a new WAL was created).
- `trim_history_scheduler_` / `flush_scheduler_` are drained by `TakeNextColumnFamily`.

Retry terminates in at most one extra prewrite call. `kWalTooBig`, which does NOT self-resolve, is explicitly excluded from the trigger set (§3.9.1 header above).

**Note on `WaitForPendingWrites`.** `DBImpl::WaitForPendingWrites` is a no-op in our config (`unordered_write=false && enable_pipelined_write=false`, db_impl.h:1926). The drain that provides memtable-swap correctness (§3.9.1a) is a *new* mechanism provided by `WaitDrain` inside `SwitchMemtable`, not a replacement of `WaitForPendingWrites`. It is needed because the leaderless path removes the WriteThread leader/group serialization that previously made no-drain safe.

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
- Any IO error in the structural switch path's inline `FlushSlotBytes` (§3.9.1) → process abort.
- This eliminates the "user was told success but later sync failed" semantic regression that the asynchronous flush model would otherwise create.
- Consistent with the existing nonblocking_wal_io durability model, which already accepts ~100ms data loss on process crash.

---

## 4. Open items and resolutions

Items 4.1, 4.2, 4.4 (all cases), 4.5, 4.6, 4.7, 4.8 are resolved. Items 4.3 and 4.9 are deferred to v2. Item 4.10 is the remaining work — the component breakdown for the implementation pass.

### 4.1 Stall handling

Stalls stay in a **separate wait zone** from the structural switch path. Stall path acquires `mutex_`, sleeps on the stall cv, and retries prewrite — no in-flight drain, no interaction with the switch path. `TrimMemtableHistory` is treated as a structural handler (it mutates memtable history) and runs under the switch path, not the stall path.

Detailed in §3.9.1 (switch path, with drain) and §3.9.2 (stall path, no drain). Ordering when both stall and switch conditions fire in the same prewrite pass: handle switch first, then retry prewrite.

### 4.2 Claim/switch race

Claim and switch are both serialized by `wal_write_mutex_` (adaptive `InstrumentedMutex`); no separate flag, no CAS race. The slot's only field is `std::atomic<uint64_t> in_flight_count`. See §3.5 for the protocol and §3.5.7 for why one mutex is enough.

### 4.3 v2 — WAL buffer overflow handling

v1 uses infinite-cap. v2 needs:

- **Soft cap** on the active buffer; when exceeded, trigger a proactive swap.
- **Coordination model:** writer-triggered swap (the writer that detects overflow becomes responsible for swapping) vs. SyncWAL-only swap (which would require frequent SyncWAL calls).
- Tentative: writer-triggered swap, mirroring how prewrite-driven WAL switch works.

Deferred to v2.

### 4.4 Recovery audit — WAL file order

WAL records are guaranteed to be in monotonic LSN order in the WAL file, on two levels:

- **Within a single buffer:** the partial-order invariant (§3.2.3) ensures that, for any two writers in the same buffer, lsn ordering matches in-buffer offset ordering. So when the buffer's bytes are appended to the WAL file (in offset order), they are also in lsn order.
- **Across a buffer boundary (A → B):** the close-drain-open sequence in §3.5.4 ensures that all `A`-writers complete their lsn advance before `B` is published as the new active buffer. By the time the first `B`-writer claims, every advance of `next_lsn` by an `A`-writer has already happened. Therefore `max(lsn in A) < min(lsn in B)`. See §3.2.4 for the full argument.

The WAL file flush order is also `A` then `B` (the §3.5.4 `SWITCHER` caller flushes `A` after publishing `B`; `B` is only flushed in some later switch). Combined, recovery sees a WAL file that is monotonic in lsn end-to-end.

Four edge cases audited below.

#### Case 2 — Partial buffer flush on crash

RocksDB's existing `log::Reader` already handles torn WAL tails via per-record CRC framing; `wal_recovery_mode` (default `kPointInTimeRecovery`) stops replay at the last valid record. Nothing about this assumes *how* the bytes were appended — one big `Append(A.bytes)` vs. many `AddRecord` calls is indistinguishable to the reader.

**Correctness obligation the new path inherits:** each writer's memcpy into `A.bytes[]` must produce bytes that are wire-format identical to what `log::Writer::AddRecord` would emit — same 7-byte header, same CRC, same 32KB-block fragmentation (FIRST/MIDDLE/LAST splits). That requires the per-writer serializer to know its absolute WAL-file offset (`file_offset_at_A_start + A_offset_for_this_writer`), not just its offset within `A.bytes[]`, so block-boundary splits land on the right boundaries. This is an implementation detail for §3.10 / §4.10, not a design change.

#### Case 3 — SwitchMemtable quiescence vs. a background flush of a prior buffer

The coordination flag `LogWriterNumber::getting_synced` (exposed as `IsSyncing()`) is set by `PrepareForSync()` and cleared by `FinishSync()`. `SwitchMemtable` (db/db_impl/db_impl_write.cc), `LockWAL` (db/db_impl/db_impl.cc), `SyncClosedLogs` (db/db_impl/db_impl_compaction_flush.cc), and `FindObsoleteFiles` (db/db_impl/db_impl_files.cc) all wait on `IsSyncing()` before touching the log::Writer's buffer.

`SyncWALNoGroup` runs through the same `PrepareForSync → {flush per log} → SyncWithoutFlush → MarkLogsSynced → FinishSync + SignalAll` sequence on every entry in `logs_`. Every existing waiter therefore covers the leaderless path's fsync for free — no new flag, no new cv, no new code paths in compaction/file-cleanup.

Structural switch (§3.9.1) flushes bytes to the OS inline under `wal_write_mutex_` without touching `IsSyncing`. Existing `IsSyncing` waiters are not affected because the inactive log produced by a structural switch has `IsSyncing=false` until the next `SyncWAL` runs.

#### Case 4 — Layering on `NonBlockingIOWritableFileWriter`

The leaderless design needs WAL-record-level slot scratch regions; `NonBlockingIOWritableFileWriter` already carries a `buf_` / `flush_buf_` ping-pong. Stacking a second ping-pong on top would create two places unflushed bytes can live.

**Decision:** modify `NonBlockingIOWritableFileWriter` in place. The existing `buf_` / `flush_buf_` pair *aliases* the two slots in leaderless mode; the class is extended with `BufferSlot` state (in_flight_count, next_wal_offset, file_offset_start) and the `active_buffer_ptr_` pointer. No subclass, no separate new-path-only writer.

**Contract the modification preserves.** In legacy mode (`use_group_commit=true`, with or without `nonblocking_wal_io`), `NonBlockingIOWritableFileWriter` behaves exactly as before. Slot fields default to benign values (in_flight_count=0, next_wal_offset=0, file_offset_start=0) that the legacy ping-pong path does not read. The constructor's `use_group_commit` parameter selects between the legacy small-flush_buf preallocation and the slot-aliased pre-grown buffers.

#### Case 1 — SyncWAL + structural switch race on the same buffer

SyncWAL (§3.4) closes the active slot with `SwitchActive` under `wal_write_mutex_`. Structural switch (§3.9.1) redirects future claims to the new log via `logs_.emplace_back(new_log)` *without* calling `SwitchActive` on the old file_writer. Serialization details:

- `IsSyncing` + `log_sync_cv_` serializes concurrent SyncWAL callers on the same log (one `IsSyncing==true` per entry at a time; late arrivers queue via `log_sync_cv_` at entry). This is the same serialization the `nonblocking_wal_io` baseline already uses for concurrent `SyncWAL` callers (db/db_write_test.cc exercises it).
- Structural switch's `while (logs_.back().IsSyncing()) log_sync_cv_.Wait()` inside `SwitchMemtable` blocks a switcher from running while a SyncWAL is mid-flush on the current back log.
- Concurrent structural switchers are additionally coalesced via `structural_switch_in_progress_` + `structural_switch_cv_` (§3.9.1b), so at most one structural switch runs the handler section at a time.

### 4.5 WriteBatch validation timing

**Where validation lives today.** Two places on the leader's side of the group-commit path:

1. `DBImpl::MergeBatch` (db/db_impl/db_impl_write.cc:1224) walks the write group and calls `WriteBatchInternal::Append(*merged_batch, writer->batch, /*WAL_only*/true)` for each follower. `Append` implicitly validates the batch (parses the header, rejects malformed batches) and the first failure fails the whole group.
2. `DBImpl::WriteToWAL` (db/db_impl/db_impl_write.cc:1280) calls `merged_batch.VerifyChecksum()` once on the merged batch, right before `log_writer->AddRecord`.

Already per-thread and unaffected: `WriteCallback` (pre-merge), and `protection_bytes_per_key` per-KV verification at memtable insert time.

**Where validation moves in the new path.** `MergeBatch` disappears entirely — no merging, each writer writes its own bytes. Both validations collapse into per-writer prewrite, **before `ClaimSlot` (§3.5):**

1. `batch->VerifyChecksum()` on the writer's own batch. This is the same call today's `WriteToWAL` makes on the merged batch, just moved to prewrite and scoped to one batch.
2. Batch format sanity — equivalent to what `WriteBatchInternal::Append` would have caught. A dedicated helper (e.g., `WriteBatchInternal::CheckContents(batch)` that parses the header and walks records without copying) is the cleanest option; at minimum a debug-only `Append` to a scratch buffer catches regressions.

Both are lockless, per-thread atomic/concurrent reads — they fit §3.8 trivially. They run **before** `ClaimSlot` because that is the point of no return: once a writer owns an in-flight slot, the design has no rollback (§3.11). A validation failure in prewrite is returned to the caller as a regular `Status` without ever touching slot state.

**What the implementation must guarantee.** No side effects before `ClaimSlot`: validation must not advance `next_lsn_`, must not touch any slot's `in_flight_count` or scratch bytes, must not write to the Link_buf ring. On failure, the writer returns the status and the caller sees exactly what it sees today on a validation failure.

### 4.6 `min_non_hole_lsn` — concrete slot encoding

Per-thread slots replaced by a Link_buf (§3.6). Each slot in the circular ring stores the end LSN of the completed batch at that position. No per-thread encoding needed — the ring is indexed by LSN, not by thread.

### 4.7 Per-thread slot registry — lifetime and allocation

Eliminated. The Link_buf (§3.6) requires no per-thread registration, no thread lifetime management, and no slot allocation. Writers store into the ring at their LSN index; the watermark advance is lock-free and opportunistic. Works with any threading model (thread pools, dynamic threads, fibers).

### 4.8 `WriteController` interaction with per-thread prewrite

The stall path (§3.9.2) is acceptable as-is. `WriteController::IsStopped()` and `NeedsDelay()` are atomic and lockless, so the fast-path check in prewrite does not contend. When a writer actually stalls, it falls into the slow path and serializes through `DBImpl::mutex_` + the stall cv. That serialization is acceptable because stall is an already-degraded mode — throughput is not the concern; correctness and liveness are.

**Correctness contract the implementation must uphold:**

- **No deadlock.** A stalled writer holds `mutex_` only while sleeping on the cv. It must not hold any buffer slot (it has not yet run `ClaimSlot` — that happens after prewrite), must not hold `wal_write_mutex_`, and must not block a switcher's drain. Fast-path writers already past their §3.1 step-3 CS must be able to complete and release their slots while other writers are stalled.
- **No crash.** The stall path must not touch buffer state, `next_lsn`, `next_wal_offset`, or the Link_buf ring. It is a pure wait; on wakeup the writer re-enters prewrite from the top and re-claims a slot normally.
- **Stall + switch interaction.** Per §3.9.2, if a stall condition and a switch condition fire in the same prewrite pass, the switch path runs first. A writer that sees only a stall condition waits on the cv; when the stall clears (via a flush completion or WriteController state change), it retries prewrite — possibly discovering a switch condition this time, which it then handles through §3.9.1.

**Unit tests required:**

1. **Stall does not deadlock the fast path.** Put N writers into the stall path (e.g., force `WriteBufferManager::ShouldStall()` to return true), while M other writers are on the fast path. The fast-path writers must complete. The stalled writers must wake and complete after the stall condition clears.
2. **Stall + concurrent switcher.** Trigger a structural switch (WAL rotate) while writers are stalled. The switcher must be able to complete `PrepareForSync` + WaitDrain + flush of the old slot (without `SwitchActive` — redirection is via `emplace_back(new_log)`; see §3.9.1) while stalled writers are asleep.
3. **Stall + concurrent SyncWAL.** A SyncWAL caller must complete (SwitchActive + WaitDrain + flush) while stalled writers are asleep on the cv. Stalled writers never held a slot, so drain is trivially satisfied; the test pins this assumption.
4. **Stall then switch condition.** A writer that wakes from stall and, on retry, sees a switch condition must correctly route through §3.9.1 — no crash, no double-claim.
5. **Many stalled writers wake simultaneously.** On broadcast, all N wake, re-run prewrite concurrently, and either claim slots (fast path) or re-stall. Verify no crashes, no lost wakeups, no Link_buf corruption.
6. **WriteController::IsStopped toggled under load.** Flip the stop flag mid-run; in-flight fast-path writers must be allowed to finish (they passed prewrite already), new writers must stall; on un-stop, stalled writers must wake.

These tests must live in the new-path-specific test file and be run under TSAN.

### 4.9 Soft cap on infinite buffer (v1 contract)

If the application stops calling SyncWAL (bug, deadlock, slow consumer), the active buffer grows unbounded.

**v1 contract:** "App must call SyncWAL regularly" is part of the v1 contract.
**v2:** add an internal SyncWAL trigger when the buffer crosses a soft threshold.

### 4.10 Components to add / modify — see companion doc

The implementation ordering, component breakdown, and PR sequencing live in `eliminate_group_commit_impl_plan.md` (Round 1 independent building blocks, Round 2 integration with the core write path). That doc cross-references each section of this design.

### 4.11 `wal_too_big`-alone edge case — deferred

`StructuralSwitchNoGroup` piggy-backs `SwitchWAL` when entered for another reason and `total_log_size_ > GetMaxTotalWalSize()` (see §3.9.1). It does NOT cover the case where `wal_too_big` is the *only* slow-path signal — e.g., a multi-CF DB whose per-CF mutable memtables stay below their size thresholds but whose accumulated immutable memtables pin many WAL files. In the legacy path, every write call pays a no-op `SwitchWAL` (early-returns once `getting_flushed=true`) which still forces the first-write real switch that unpins the oldest WAL. The leaderless path has no such per-write check.

**Why deferred:** resurrecting `kWalTooBig` as a prewrite trigger requires either bounded retry semantics (break out after N attempts) or a distinct one-shot branch in the outer loop — both diverge from the clean "all structural reasons self-resolve, retry is livelock-free" model we adopted. The edge case is rare in practice (requires write_buffer_size >> max_total_wal_size / num_cfs) and its legacy handling is marginal anyway (repeated no-op calls).

**Follow-up:** revisit alongside the Round-3 stall / backpressure work (impl plan 2.5+), which will need to think about per-condition retry semantics in general.

### 4.12 Range tombstone rejection — implementation constraint

`WriteImplNoGroup` rejects any batch with `WriteBatch::HasDeleteRange()` as `Status::NotSupported`. Rationale:

- `SwitchMemtable` calls `MemTable::ConstructFragmentedRangeTombstones()` on the about-to-be-immutable memtable between the `wal_write_mutex_` section and the memtable swap.
- In the legacy path, the leader's write-queue ownership makes this race-free — no concurrent writer can touch the memtable.
- In the leaderless path, concurrent writers may still be inserting regular keys at that moment. `ConstructFragmentedRangeTombstones` is a no-op when `is_range_del_table_empty_==true`, and regular-key inserts don't touch range-del state, so the concurrent-insert window is safe *for regular keys alone*.
- Allowing range tombstones would create a genuine data race on the range-del table between in-flight writers and the switcher.

Rejecting at entry keeps `is_range_del_table_empty_` always true for leaderless-written memtables, preserving the no-op invariant. Lifting the restriction would require either serializing range-tombstone inserts through the switcher or replacing `ConstructFragmentedRangeTombstones` with a lazy build.

### 4.13 `log_empty_` synchronization — deferred, chosen direction: force `creating_new_log=true` in leaderless

**Current state.** `log_empty_` is a plain `bool` on `DBImpl`.

| Site | Value | Lock held |
|---|---|---|
| `DBImpl` ctor | `true` | — |
| `DBImpl::Recover` | `false` | `mutex_` |
| `WriteToWAL` (legacy) | `false` | `wal_write_mutex_` |
| `SwitchMemtable` (after new log pushed) | `true` | `wal_write_mutex_` |
| `WriteImplNoGroup` (leaderless, added in 2.3) | `false` | `wal_write_mutex_` |
| `SwitchMemtable` read `creating_new_log = !log_empty_` | — | `wal_write_mutex_` iff `two_write_queues_` else **no lock** |

**The race.** In the leaderless path `two_write_queues_` is false, so `SwitchMemtable`'s read of `log_empty_` is not under any lock synchronized with `WriteImplNoGroup`'s write. They are concurrent on different locks (switcher holds `mutex_`; writer holds `wal_write_mutex_`) and the pre-leaderless comment at `db_impl.h:2386-2391` — "it is currently updated/read only in write_thread_, it can be accessed without any locks" — is no longer true in the leaderless path (no write_thread_; many writers update `log_empty_` concurrently via `wal_write_mutex_`).

Formally a data race on a non-atomic `bool`. Practically the window is tiny — `wal_write_mutex_` release and `mutex_` acquire both imply full memory barriers on common hardware — but the correctness argument depends on implementation details rather than the C++ memory model.

**Consequence if the race fires — narrower than it first appears.** If `SwitchMemtable` reads stale `true` when the writer has just set `false`:
- `creating_new_log = false`.
- The whole `if (s.ok() && creating_new_log)` block — including `WaitDrain(A)` + `FlushSlotBytes(A)` + `emplace_back(new_log)` — is skipped.
- The memtable swap at the tail of `SwitchMemtable` still runs.
- In-flight writers that claimed on the old slot continue landing in the OLD WAL and race with the memtable swap.

However, the §3.9.1a data-loss scenario (OLD WAL deleted because OLD memtable flushed while W's entry lives in NEW memtable) requires `creating_new_log=true`, i.e. OLD and NEW memtables pointing to **different** WALs. The stale-read bug produces `creating_new_log=false`, which keeps `logfile_number_` unchanged: OLD and NEW memtables share the same WAL, so that WAL stays pinned until both flush. W's record, whether its in-memory copy ends up in OLD or NEW memtable, is replayable from the (single) WAL.

So the leaked race is formal C++ UB but not a data-loss bug. Still worth eliminating for TSAN cleanliness and to stop depending on "the race is narrow in practice" reasoning.

**Chosen direction (deferred): Option 3 — force `creating_new_log=true` in the leaderless path.**

`SwitchMemtable` unconditionally takes the new-log branch when `!use_group_commit_`. Specifically:

- The read `bool creating_new_log = !log_empty_` is bypassed; leaderless substitutes `true`.
- A fresh `new_log_number` is always allocated and `CreateWAL` always runs.
- The `wal_write_mutex_` block at db_impl_write.cc:2157-2252 always executes, so `WaitDrain(A)` + `FlushSlotBytes(A)` + `emplace_back(new_log)` always run.
- `logfile_number_` always advances; `log_empty_=true` is always set inside the `wal_write_mutex_` block (same as today's `creating_new_log=true` branch).
- The "log is empty — reuse the file" optimization is given up for the leaderless path.

**Why Option 3 over Options 1/2.**

Options considered:
1. *Read `log_empty_` under `wal_write_mutex_` when `!use_group_commit_`.* Minimal patch but only fixes the bool read. New writers arriving between the read and the `wal_write_mutex_` block still land in the old slot; the design continues to rely on "the narrow-window analysis above is benign" rather than eliminating the decision.
2. *Make `log_empty_` atomic.* Same theoretical coverage as Option 1, more churn across the legacy path, same residual window.
3. *Force `creating_new_log=true` in leaderless.* **Chosen.** Removes the log_empty_ read from the leaderless decision path entirely, so neither the bool race nor the "writer arrives after the read" window exists. The memtable-swap barrier (WaitDrain) is then guaranteed to run on every leaderless `SwitchMemtable`, without needing to reason about which branch was taken.

Cost: in the rare back-to-back `SwitchMemtable`-without-writes case (e.g. multiple CFs queued by `flush_scheduler_` in the same pass), we allocate a fresh file number and create an empty WAL file that obsolete-file cleanup will delete later. `SwitchMemtable` is not hot-path, so the cost is a few extra file numbers and an empty WAL file per back-to-back switch — acceptable.

**Open question still worth auditing alongside.** `alive_log_files_` and `logfile_number_` carry the same "protected by `wal_write_mutex_` only when `two_write_queues_`, otherwise by `mutex_` + write_thread_" pattern per the header comments. `WriteImplNoGroup` updates `alive_log_files_.back().AddSize(byte_count)` under `wal_write_mutex_`. Option 3 removes the `log_empty_` read from the leaderless path but does **not** automatically fix reads of `alive_log_files_`/`logfile_number_` elsewhere — any leaderless reader that inherited the legacy "no lock needed without two_write_queues" doctrine is still racy. Needs its own audit.

**Status.** Discussion point, not yet implemented. Track with §4.13 when the Round 2 work resumes.

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
- `WriteBatch::HasDeleteRange()` (range tombstones) — see §4.12
- `wal_too_big` as a standalone prewrite trigger — see §4.11
- Merge operator (`WriteBatch::HasMerge()`) — requires read-before-write, conflicts with concurrent memtable inserts

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
