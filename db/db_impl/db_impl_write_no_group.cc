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
#include "db/log_record_serializer.h"
#include "db/log_writer.h"
#include "db/preprocess_write_lockless.h"
#include "db/write_batch_internal.h"
#include "db/write_callback.h"
#include "db/write_thread.h"
#include "file/nonblocking_io_writable_file_writer.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Maps a non-ok PrewriteReason to a user-facing Status.  Round-2 stub:
// every slow-path reason returns NotSupported, pending the structural
// switch / stall integration in Round 3 (§3.9.1 / §3.9.2 / §3.8).
// kBatchCorrupt already carries the decoded validation Status in pw.
Status MapPrewriteReasonToStatus(const PrewriteResult& pw) {
  switch (pw.reason) {
    case PrewriteReason::kOk:
      assert(false);
      return Status::OK();
    case PrewriteReason::kBatchTooLarge:
      return Status::Busy(
          "WriteBatch exceeds Link_buf ring capacity; split the batch");
    case PrewriteReason::kBatchCorrupt:
      return pw.validation_status;
    case PrewriteReason::kDbStopped:
    case PrewriteReason::kWalTooBig:
    case PrewriteReason::kWbmShouldFlush:
    case PrewriteReason::kTrimHistoryPending:
    case PrewriteReason::kFlushPending:
    case PrewriteReason::kWriteStalled:
    case PrewriteReason::kWbmShouldStall:
    case PrewriteReason::kNeedLogSync:
      return Status::NotSupported(
          "use_group_commit=false slow-path reasons are not wired yet "
          "(Round-2 stub)");
  }
  return Status::NotSupported("unknown PrewriteReason");
}

}  // namespace

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
  assert(lsn_offset_allocator_ != nullptr);

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

  const uint64_t batch_lsn_count =
      static_cast<uint64_t>(WriteBatchInternal::Count(my_batch));
  const size_t payload_size = WriteBatchInternal::ByteSize(my_batch);

  // §3.8 lockless prewrite.  need_log_sync=false because sync writes are
  // rejected above — only SyncWAL() triggers the log-sync path.
  PrewriteResult pw = PreprocessWriteLockless(
      this, write_options, my_batch, batch_lsn_count, /*need_log_sync=*/false);
  if (pw.reason != PrewriteReason::kOk) {
    return MapPrewriteReasonToStatus(pw);
  }

  // Callback runs before any slot claim or LSN allocation so failures do
  // not leave holes in the LSN space.
  if (callback != nullptr) {
    Status cb = callback->Callback(this);
    if (!cb.ok()) {
      return cb;
    }
  }

  // Snapshot the current WAL writer.  `logs_` is protected by
  // log_write_mutex_ (see db_impl.h:2420 and SyncWAL at db_impl.cc:1471
  // for the established pattern); concurrent emplace_back from a
  // background SwitchMemtable would otherwise race with back().
  //
  // For 2.1 scope the function is not wired into WriteImpl, so the
  // only caller is tests that do not drive flushes — logs_.size() is
  // always 1 and logs_.back() can never be popped or demoted.  The
  // assert below traps any scope violation.  Once piece 2.3 lands
  // (structural switch path), the switcher's Phase-2 drain of the
  // slot's in_flight_count will hold off emplace_back for the
  // lifetime of any successful ClaimSlot(), making the bare
  // logs_.back() read safe without this mutex — at that point the
  // snapshot can move below ClaimSlot and the mutex dropped.
  log::Writer* log_writer;
  NonBlockingIOWritableFileWriter* file_writer;
  {
    InstrumentedMutexLock l(&log_write_mutex_);
    assert(logs_.size() == 1);
    log_writer = logs_.back().writer;
    file_writer =
        static_cast<NonBlockingIOWritableFileWriter*>(log_writer->file());
  }
  NonBlockingIOWritableFileWriter::BufferSlot* slot = file_writer->ClaimSlot();

  const bool recycle = immutable_db_options_.recycle_log_file_num > 0;

  // §3.2 paired advance.  byte_count is computed inside the spinlock
  // from (slot->file_offset_start + *next_wal_offset) so it matches the
  // abs_file_offset we later hand to SerializeRecord.
  LsnOffsetAllocator::AdvanceResult adv =
      lsn_offset_allocator_->AdvanceForSerialize(
          batch_lsn_count, payload_size, slot->file_offset_start, recycle,
          &slot->next_wal_offset);

  // §3.6 post-advance ring-capacity guard.  Fires only if a writer has
  // been stuck for an extraordinary duration; aborts the process per
  // §3.11.
  link_buf_->CheckRingCapacityOrAbort(adv.start_lsn, batch_lsn_count);

  // Slot bytes hard-cap check.  Violations are a contract failure (app
  // must call SyncWAL regularly — §4.9); abort rather than silently
  // corrupting the file.
  if (adv.start_offset + adv.byte_count > file_writer->SlotBytesCapacity()) {
    std::abort();
  }

  // Publish sequence into the batch header before memtable insert —
  // InsertInto reads it back from the Writer we'll construct below.
  WriteBatchInternal::SetSequence(my_batch, adv.start_lsn);

  // Memtable insert.  `true /*concurrent_memtable_writes*/` routes
  // inserts through MemTable::AddConcurrently; `hint_per_batch` honors
  // write_options.memtable_insert_hint_per_batch for skiplist
  // insertion-hint reuse.  The Writer here is a parameter carrier
  // only — it is never enlisted with WriteThread.
  WriteThread::Writer writer(write_options, my_batch, /*callback=*/nullptr,
                             /*log_ref=*/0, /*disable_memtable=*/false);
  writer.sequence = adv.start_lsn;

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
  const uint64_t abs_file_offset = slot->file_offset_start + adv.start_offset;
  char* dst = file_writer->SlotBytesStart(slot) + adv.start_offset;
  const size_t wrote = log::SerializeRecord(
      payload, abs_file_offset, log_writer->get_log_number(), recycle, dst);
  // ComputeSerializedRecordSize inside AdvanceForSerialize must return
  // the same size as SerializeRecord actually writes — any mismatch
  // would corrupt the WAL.  Assert in debug, abort in release.
  assert(wrote == adv.byte_count);
  if (wrote != adv.byte_count) {
    std::abort();
  }
  // Keep filesize_ honest for WAL-too-big / flush-invariant checks — writers
  // bypass Append() so they must advance it manually.  Done after the
  // serialize so a partial SerializeRecord abort above never leaves
  // filesize_ over-counted.
  file_writer->AdvanceFileSize(static_cast<size_t>(adv.byte_count));

  // §3.6 mark this batch's LSN range complete and try to advance the
  // watermark.  Must happen before ReleaseSlot so the switcher's
  // drained view reflects our completion.
  link_buf_->Store(adv.start_lsn, adv.start_lsn + batch_lsn_count);
  const uint64_t new_watermark = link_buf_->AdvanceWatermark();

  // Publish reader visibility.  Writers complete out of order, so we
  // cannot use `start_lsn + batch_lsn_count - 1` directly — a lagging
  // writer would regress last_sequence_.  Instead publish the
  // monotonic Link_buf watermark (minus one, since the watermark is
  // the next-free LSN), which reaches this batch only after every
  // earlier batch has completed — matching the legacy group-commit
  // visibility rule.  See design §3.6.
  if (new_watermark > 0) {
    versions_->SetLastSequenceAtLeast(new_watermark - 1);
  }

  file_writer->ReleaseSlot(slot);

  if (log_used != nullptr) {
    *log_used = log_writer->get_log_number();
  }
  if (seq_used != nullptr) {
    *seq_used = adv.start_lsn;
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
