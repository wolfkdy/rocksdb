//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Leaderless WAL write path for use_group_commit=false.  See
// eliminate_group_commit_design.md §3.1 and §3.8.

#include <cstdlib>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/log_record_serializer.h"
#include "db/log_writer.h"
#include "db/preprocess_write_lockless.h"
#include "db/write_batch_internal.h"
#include "db/write_callback.h"
#include "db/write_thread.h"
#include "file/nonblocking_io_writable_file_writer.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Classify a non-kOk PrewriteReason.  Only referenced from assertions,
// so guarded for release builds.
[[maybe_unused]] bool IsStructuralReason(PrewriteReason r) {
  return r == PrewriteReason::kWalTooBig ||
         r == PrewriteReason::kWbmShouldFlush ||
         r == PrewriteReason::kTrimHistoryPending ||
         r == PrewriteReason::kFlushPending;
}
[[maybe_unused]] bool IsStallReason(PrewriteReason r) {
  return r == PrewriteReason::kWriteStalled ||
         r == PrewriteReason::kWbmShouldStall;
}

}  // namespace

// RAII: winner's cleanup for structural_switch_in_progress_.
// Destructor flips the flag back to false and broadcasts
// structural_switch_cv_ so waiters resume.  Abort paths (§3.11) go
// through std::abort() which kills the process without running
// destructors — waiters of a crashing DB are not a correctness
// concern.
DBImpl::StructuralSwitchFlagGuard::StructuralSwitchFlagGuard(DBImpl* db)
    : db_(db) {}

DBImpl::StructuralSwitchFlagGuard::~StructuralSwitchFlagGuard() {
  {
    InstrumentedMutexLock l(&db_->mutex_);
    db_->structural_switch_in_progress_ = false;
  }
  db_->structural_switch_cv_.SignalAll();
}

Status DBImpl::WriteImplNoGroup(const WriteOptions& write_options,
                                WriteBatch* my_batch, WriteCallback* callback,
                                uint64_t* log_used, uint64_t* seq_used) {
  // Mode invariants — enforced at DB open and re-asserted here.
  assert(!immutable_db_options_.use_group_commit);
  assert(immutable_db_options_.nonblocking_wal_io);
  assert(!immutable_db_options_.two_write_queues);
  assert(!immutable_db_options_.unordered_write);
  assert(!immutable_db_options_.enable_pipelined_write);
  assert(link_buf_ != nullptr);

  if (my_batch == nullptr) {
    return Status::Corruption("Batch is nullptr!");
  }
  if (write_options.disableWAL) {
    return Status::NotSupported(
        "disableWAL is not supported when use_group_commit=false");
  }
  if (write_options.sync) {
    return Status::NotSupported(
        "sync writes are not supported when use_group_commit=false; "
        "call SyncWAL() instead");
  }
  // Merge requires read-before-write, which conflicts with concurrent
  // memtable inserts.  Reject upfront — same constraint legacy
  // allow_concurrent_memtable_write path enforces via
  // concurrent_prepare assertions.
  if (my_batch->HasMerge()) {
    return Status::NotSupported(
        "Merge is not supported when use_group_commit=false");
  }
  // Range tombstones are not supported in the leaderless path.
  // Rationale: SwitchMemtable calls ConstructFragmentedRangeTombstones
  // on the now-immutable memtable between the wal_write_mutex_ section
  // and the memtable swap; in the legacy path the leader's write-queue
  // ownership makes this race-free, but here there are no queues — in-
  // flight writers may still be inserting regular keys at that moment.
  // Range-tombstone inserts would additionally touch the range-del
  // table that ConstructFragmentedRangeTombstones iterates, creating a
  // true data race.  Rejecting range tombstones up front keeps
  // is_range_del_table_empty_ always true for memtables written by
  // this path, so ConstructFragmentedRangeTombstones is a no-op and
  // the concurrent-insert window is safe for regular keys alone.
  if (my_batch->HasDeleteRange()) {
    return Status::NotSupported(
        "DeleteRange is not supported when use_group_commit=false");
  }

  const uint64_t batch_lsn_count =
      static_cast<uint64_t>(WriteBatchInternal::Count(my_batch));
  const size_t payload_size = WriteBatchInternal::ByteSize(my_batch);

  // §3.8 lockless prewrite, wrapped in a slow-path loop.  Validation
  // failures return straight away; structural switches hand off to
  // StructuralSwitchNoGroup (§3.9.1) and then `continue` to retry
  // prewrite — safe because the three remaining structural reasons
  // (kWbmShouldFlush, kTrimHistoryPending, kFlushPending) are all
  // self-resolving after one real switch (see the "self-resolving"
  // note in StructuralSwitchNoGroup).  kWalTooBig is intentionally
  // NOT a structural reason in PreprocessWriteLockless (the condition
  // is not self-resolving, so retrying would livelock), but
  // StructuralSwitchNoGroup does still piggy-back a SwitchWAL when
  // it is entered for another reason and wal_too_big is also true.
  //
  // need_log_sync=false because WriteOptions::sync is rejected above —
  // durability is the SyncWAL() caller's responsibility.
  bool proceed_to_fast_path = false;
  while (!proceed_to_fast_path) {
    PrewriteResult pw = PreprocessWriteLockless(
        this, write_options, my_batch, batch_lsn_count,
        /*need_log_sync=*/false);
    switch (pw.reason) {
      case PrewriteReason::kOk:
        proceed_to_fast_path = true;
        break;
      case PrewriteReason::kBatchTooLarge:
        return Status::Busy(
            "WriteBatch exceeds Link_buf ring capacity; split the batch");
      case PrewriteReason::kBatchCorrupt:
        return pw.validation_status;
      case PrewriteReason::kDbStopped: {
        InstrumentedMutexLock l(&mutex_);
        return error_handler_.GetBGError();
      }
      case PrewriteReason::kNeedLogSync:
        // need_log_sync is hard-coded to false above; this branch is
        // unreachable unless PreprocessWriteLockless is restructured.
        assert(false);
        return Status::NotSupported(
            "leaderless path does not honor WriteOptions::sync");
      case PrewriteReason::kWalTooBig:
        // PreprocessWriteLockless never returns kWalTooBig today — its
        // cond.wal_too_big is hard-wired to false.  The enum value is
        // preserved for future re-wiring (when / if we decide to
        // promote wal_too_big back into prewrite with bounded retry
        // semantics).  Treat as unreachable.
        assert(false);
        return Status::NotSupported(
            "kWalTooBig is not a prewrite trigger in the leaderless path");
      case PrewriteReason::kWbmShouldFlush:
      case PrewriteReason::kTrimHistoryPending:
      case PrewriteReason::kFlushPending: {
        assert(IsStructuralReason(pw.reason));
        Status s = StructuralSwitchNoGroup();
        if (!s.ok()) {
          return s;
        }
        // Retry prewrite.  These conditions self-resolve after one
        // switch, so the retry either sees kOk (common) or a different
        // condition (e.g. a fresh arrival of kWbmShouldFlush from new
        // writes between our switch and the retry — unlikely but
        // handled correctly by another coalesced switch).
        continue;
      }
      case PrewriteReason::kWriteStalled:
      case PrewriteReason::kWbmShouldStall:
        // §3.9.2 — Round-3 (piece 2.5).  Stubbed as NotSupported so the
        // structural-switch path (2.3) can land without a dependency on
        // the stall wait wiring.  When 2.5 lands, dispatch here should
        // wait on the appropriate condition and `continue` (retry prewrite).
        assert(IsStallReason(pw.reason));
        return Status::NotSupported(
            "use_group_commit=false stall path is not yet wired "
            "(eliminate_group_commit impl plan 2.5)");
    }
  }

  // Callback runs after prewrite (so it sees the post-switch state) and
  // before any slot claim or LSN allocation — failures must not leave
  // holes in the LSN space.
  if (callback != nullptr) {
    Status cb = callback->Callback(this);
    if (!cb.ok()) {
      return cb;
    }
  }

  const bool recycle = immutable_db_options_.recycle_log_file_num > 0;

  // §3.2 Single critical section under wal_write_mutex_:
  //   - snapshot the current log::Writer (logs_.back())
  //   - ClaimSlot to pin the active buffer for the lifetime of this write
  //   - allocate (start_lsn, start_offset, byte_count) — paired so the
  //     partial-order invariant (lsn[i]<lsn[j] ⇔ off[i]<off[j]) holds.
  // The CS is short and IO-free; the mutex is adaptive so it spins
  // briefly before sleeping.
  log::Writer* log_writer;
  NonBlockingIOWritableFileWriter::BufferSlot* slot;
  uint64_t start_lsn;
  uint64_t start_offset;
  uint64_t byte_count;
  uint64_t abs_file_offset;
  {
    InstrumentedMutexLock l(&wal_write_mutex_);
    log_writer = logs_.back().writer;
    auto* fw_in_cs = static_cast<NonBlockingIOWritableFileWriter*>(
        log_writer->file());
    slot = fw_in_cs->ClaimSlot();
    start_lsn = next_lsn_;
    start_offset = slot->next_wal_offset;
    abs_file_offset = slot->file_offset_start + start_offset;
    byte_count = static_cast<uint64_t>(log::ComputeSerializedRecordSize(
        payload_size, abs_file_offset, recycle));
    next_lsn_ += batch_lsn_count;
    slot->next_wal_offset += byte_count;
    // alive_log_files_'s back() changes only when SwitchMemtable calls
    // push_back under wal_write_mutex_, so this update is safe here.
    // log_empty_ is intentionally NOT written: SwitchMemtable forces
    // creating_new_log=true in the leaderless path (§4.13) so no reader
    // consumes it.  PreprocessWriteLockless's wal_too_big check is
    // hard-wired off (§4.11) and reads total_log_size_, not log_empty_.
    assert(!alive_log_files_.empty());
    alive_log_files_.back().AddSize(byte_count);
  }
  // log_writer->file() and its log_number are immutable after construction,
  // so read them outside the CS on the captured pointer.
  auto* file_writer =
      static_cast<NonBlockingIOWritableFileWriter*>(log_writer->file());
  const uint64_t log_number = log_writer->get_log_number();
  total_log_size_.fetch_add(byte_count, std::memory_order_relaxed);

  // Writer has released wal_write_mutex_ but still holds an in-flight
  // count on slot.  A concurrent structural switcher reaching
  // SwitchMemtable's leaderless branch will block in WaitDrain until
  // this writer completes InsertInto + ReleaseSlot.  Tests use this
  // sync point to park the writer between claim and InsertInto in
  // order to observe the memtable-swap barrier deterministically.
  TEST_SYNC_POINT("DBImpl::WriteImplNoGroup:AfterClaimBeforeInsert");

  // §3.6 ring-capacity guard — fires only if a writer has been stuck for
  // an extraordinary duration; aborts per §3.11.
  link_buf_->CheckRingCapacityOrAbort(start_lsn, batch_lsn_count);

  // Slot bytes hard-cap.  Violations are a contract failure (app must
  // call SyncWAL regularly — §4.9); abort rather than corrupt the file.
  if (start_offset + byte_count > file_writer->SlotBytesCapacity()) {
    std::abort();
  }

  // Publish sequence into the batch header before memtable insert —
  // InsertInto reads it back from the Writer we'll construct below.
  WriteBatchInternal::SetSequence(my_batch, start_lsn);

  // Memtable insert (mutex-free).  Slot remains pinned by ClaimSlot's
  // in-flight count, so a concurrent switcher cannot retire the slot
  // bytes underneath us.
  WriteThread::Writer writer(write_options, my_batch, /*callback=*/nullptr,
                             /*log_ref=*/0, /*disable_memtable=*/false);
  writer.sequence = start_lsn;

  ColumnFamilyMemTablesImpl cf_memtables(versions_->GetColumnFamilySet());
  Status mt = WriteBatchInternal::InsertInto(
      &writer, writer.sequence, &cf_memtables, &flush_scheduler_,
      &trim_history_scheduler_, write_options.ignore_missing_column_families,
      /*log_number=*/0, this, /*concurrent_memtable_writes=*/true,
      /*seq_per_batch=*/false, /*batch_cnt=*/0, batch_per_txn_,
      write_options.memtable_insert_hint_per_batch);
  // §3.11: any post-claim failure is unrecoverable — the LSN space has
  // been advanced and the slot bytes would leave a hole if we returned.
  if (!mt.ok()) {
    std::abort();
  }

  // §3.10 serialize WAL bytes into the slot's scratch region.
  Slice payload = WriteBatchInternal::Contents(my_batch);
  char* dst = file_writer->SlotBytesStart(slot) + start_offset;
  const size_t wrote = log::SerializeRecord(payload, abs_file_offset,
                                            log_number, recycle, dst);
  // ComputeSerializedRecordSize must equal SerializeRecord's actual
  // write size — any mismatch would corrupt the WAL.
  assert(wrote == byte_count);
  if (wrote != byte_count) {
    std::abort();
  }
  // Keep filesize_ honest for WAL-too-big / flush-invariant checks —
  // writers bypass Append() so they must advance it manually.
  file_writer->AdvanceFileSize(static_cast<size_t>(byte_count));

  // §3.6 mark this batch's LSN range complete and advance watermark
  // before releasing the slot — switcher's drained view must reflect
  // our completion.
  link_buf_->Store(start_lsn, start_lsn + batch_lsn_count);
  const uint64_t new_watermark = link_buf_->AdvanceWatermark();

  // Publish reader visibility via the Link_buf watermark (minus one,
  // since the watermark is the next-free LSN).  This advances only after
  // every earlier batch has completed — matching legacy group-commit
  // visibility (design §3.6).
  if (new_watermark > 0) {
    versions_->SetLastSequenceAtLeast(new_watermark - 1);
  }

  file_writer->ReleaseSlot(slot);

  if (log_used != nullptr) {
    *log_used = log_number;
  }
  if (seq_used != nullptr) {
    *seq_used = start_lsn;
  }
  return Status::OK();
}

Status DBImpl::StructuralSwitchNoGroup() {
  // §3.9.1 — structural slow path for the leaderless write path.
  //
  // Coalescing model.  At most one writer at a time ("winner") runs
  // the handler + flush sequence; other arrivers ("losers") wait on
  // structural_switch_cv_ until the winner clears
  // structural_switch_in_progress_, then return OK so the caller's
  // outer WriteImplNoGroup loop re-runs PreprocessWriteLockless.  The
  // three remaining structural reasons (kWbmShouldFlush,
  // kTrimHistoryPending, kFlushPending) are all self-resolving after
  // one real switch, so the retry is guaranteed to terminate in one
  // extra prewrite call (no livelock).  kWalTooBig is handled by the
  // piggy-backed SwitchWAL check below, not as its own trigger — see
  // PreprocessWriteLockless for the rationale.
  //
  // Why coalesce at all?  Without the flag, two writers entering
  // concurrently can both capture creating_new_log=true inside
  // SwitchMemtable during the window where it releases mutex_ to call
  // CreateWAL.  That produces a "double switch" — two fresh WAL
  // files, with the second immediately orphaning the first.  The
  // flag closes that race at its source.
  //
  // Lock ordering.  Only one of {mutex_, wal_write_mutex_} is held at
  // a time here.  Inside the winner's handler section we hold mutex_
  // only: SwitchMemtable takes wal_write_mutex_ internally for the
  // {SwitchActive, PrepareForSync, emplace_back(new_log)} CS, so we
  // must NOT be holding it ourselves or the non-recursive
  // InstrumentedMutex deadlocks.  wal_write_mutex_ is taken alone at
  // the tail for MarkLogsSynced.

  // ---- Entry: decide winner vs. loser ----
  {
    InstrumentedMutexLock l(&mutex_);
    if (structural_switch_in_progress_) {
      // Loser.  Wait for the winner's SignalAll, then return OK and
      // let the caller retry prewrite.  If the winner's switch
      // resolved our triggering condition (the common case — see the
      // "self-resolving" note above), prewrite will return kOk and
      // the writer proceeds to the fast path.  Otherwise the caller
      // re-enters StructuralSwitchNoGroup and may win this time.
      while (structural_switch_in_progress_) {
        TEST_SYNC_POINT("DBImpl::StructuralSwitchNoGroup:LoserAboutToWait");
        structural_switch_cv_.Wait();
      }
      return Status::OK();
    }
    structural_switch_in_progress_ = true;
  }
  TEST_SYNC_POINT("DBImpl::StructuralSwitchNoGroup:WinnerAfterClaim");

  // RAII cleanup: destructor on scope exit clears the flag and
  // broadcasts the cv so waiters resume.  Covers every non-abort
  // return path below (status errors, nullptr-flush early exit,
  // ApplyWALToManifest failure, normal success).
  StructuralSwitchFlagGuard guard(this);

  // ---- Winner: run handlers ----
  WriteContext write_context;
  Status status;
  {
    InstrumentedMutexLock l(&mutex_);
    // Re-check each condition under mutex_.  The order mirrors
    // DBImpl::PreprocessWrite (db_impl_write.cc:1153) so coalescing
    // matches the legacy ladder.  We do NOT call WaitForPendingWrites()
    // — it is a no-op in our config (unordered_write=false &&
    // enable_pipelined_write=false, db_impl.h:1926) and the in-flight
    // drain needed for memtable-swap correctness lives inside
    // SwitchMemtable's post-wal_write_mutex_ WaitDrain.
    if (error_handler_.IsDBStopped()) {
      status = error_handler_.GetBGError();
    }
    // Piggy-backed kWalTooBig: even though prewrite does not trigger
    // on it (see PreprocessWriteLockless comment), we opportunistically
    // call SwitchWAL when we're already in the handler section for
    // some other reason.  SwitchWAL is a no-op once
    // alive_log_files_.begin()->getting_flushed is true, so repeated
    // entries self-coalesce cheaply.
    if (status.ok() &&
        total_log_size_.load(std::memory_order_relaxed) >
            GetMaxTotalWalSize()) {
      const ColumnFamilySet* const column_families =
          versions_->GetColumnFamilySet();
      assert(column_families);
      size_t num_cfs = column_families->NumberOfColumnFamilies();
      assert(num_cfs >= 1);
      if (num_cfs > 1) {
        status = SwitchWAL(&write_context);
      }
    }
    if (status.ok() && write_buffer_manager_ != nullptr &&
        write_buffer_manager_->ShouldFlush()) {
      status = HandleWriteBufferManagerFlush(&write_context);
    }
    if (status.ok() && !trim_history_scheduler_.Empty()) {
      status = TrimMemtableHistory(&write_context);
    }
    if (status.ok() && !flush_scheduler_.Empty()) {
      status = ScheduleFlushes(&write_context);
    }
  }
  // The handlers completed everything SwitchMemtable needed (WaitDrain
  // + FlushSlotBytes under wal_write_mutex_; memtable swap under
  // mutex_).  No outside-the-lock flush to do — durability of the old
  // log is deferred to the next SyncWAL, matching the legacy model
  // (PosixWritableFile::Flush is a no-op; SwitchMemtable's legacy
  // WriteBuffer only does write() to OS, no fsync).
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
