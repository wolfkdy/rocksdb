//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// SyncWAL variant for use_group_commit=false.  See design §3.4.

#include <cstdlib>

#include "db/db_impl/db_impl.h"
#include "db/log_writer.h"
#include "db/version_edit.h"
#include "file/nonblocking_io_writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/statistics.h"
#include "rocksdb/io_status.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

// Shape mirrors DBImpl::SyncWAL (db_impl.cc:1464) — same variable names,
// same iteration idiom, same front-only IsSyncing wait, same autovector
// collection — so a reader familiar with legacy SyncWAL recognizes the
// skeleton.  Only the buffer-level primitives differ, per §3.9.1:
//   - Back log: SwitchActive (leaderless analogue of SwapBuffer) +
//     WaitDrain + FlushSlotBytes (leaderless analogue of
//     FlushSwappedBuffer).  SwapBuffer/FlushSwappedBuffer operate on
//     buf_/flush_buf_ with AlignedBuffer::CurrentSize(); FlushSlotBytes
//     operates on a BufferSlot with slot->next_wal_offset.
//   - Inactive logs: no per-log buffer-switch or flush; bytes were
//     pushed to OS by SwitchMemtable's inline FlushSlotBytes at
//     rotation time.  Only SyncWithoutFlush is needed.
// Error handling also diverges: §3.11 requires abort on IO failure
// here, where legacy returns IOError and sets BG error.
Status DBImpl::SyncWALNoGroup() {
  TEST_SYNC_POINT("DBImpl::SyncWAL:Begin");

  assert(!immutable_db_options_.use_group_commit);
  assert(immutable_db_options_.nonblocking_wal_io);

  autovector<log::Writer*, 1> logs_to_sync;
  bool need_log_dir_sync;
  uint64_t current_log_number;

  // Back log's active slot at the time of SwitchActive — frozen and
  // ready for FlushSlotBytes outside the CS.  Leaderless-only state.
  NonBlockingIOWritableFileWriter* back_fw = nullptr;
  NonBlockingIOWritableFileWriter::BufferSlot* back_A = nullptr;

  {
    InstrumentedMutexLock l(&wal_write_mutex_);
    assert(!logs_.empty());

    // Full serialization: only one SyncWALNoGroup caller runs the
    // sync body at a time.  Concurrent callers queue here.  See
    // design doc §3.4.2 — the earlier front-only IsSyncing wait
    // let concurrent callers race with each other's MarkLogsSynced
    // erasures, which could leave a waiter with logs_to_sync empty.
    while (sync_wal_in_progress_) {
      log_sync_cv_.Wait();
    }
    sync_wal_in_progress_ = true;

    current_log_number = logfile_number_;

    // First check that logs are safe to sync in background.
    for (auto it = logs_.begin();
         it != logs_.end() && it->number <= current_log_number; ++it) {
      if (!it->writer->file()->writable_file()->IsSyncThreadSafe()) {
        sync_wal_in_progress_ = false;
        log_sync_cv_.SignalAll();
        return Status::NotSupported(
            "SyncWAL() is not supported for this implementation of WAL file",
            immutable_db_options_.allow_mmap_writes
                ? "try setting Options::allow_mmap_writes to false"
                : Slice());
      }
    }
    for (auto it = logs_.begin();
         it != logs_.end() && it->number <= current_log_number; ++it) {
      auto& log = *it;
      log.PrepareForSync();
      logs_to_sync.push_back(log.writer);
    }

    // Leaderless analogue of legacy's "SwapBuffer on each": SwitchActive
    // on the back log only.  SwitchActive on an inactive file_writer
    // would fire its `active == A` precondition assert, and inactive
    // logs have no per-log work to do here anyway (their bytes are
    // already in OS cache from SwitchMemtable's inline FlushSlotBytes).
    //
    // logs_to_sync is guaranteed non-empty here: we hold
    // sync_wal_in_progress_, so no concurrent caller could have
    // erased the back log between our current_log_number snapshot
    // above and this point.
    assert(!logs_to_sync.empty());
    log::Writer* back = logs_to_sync.back();
    back_fw = static_cast<NonBlockingIOWritableFileWriter*>(back->file());
    back_A = back_fw->ActiveSlot();
    back_fw->SwitchActive(back_A, back_fw->OtherSlot(back_A));

    need_log_dir_sync = !log_dir_synced_;
  }

  TEST_SYNC_POINT("DBImpl::SyncWALNoGroup:AfterPrepareForSync");
  RecordTick(stats_, WAL_FILE_SYNCED);

  // Drain in-flight writers on the back log's A before flushing.
  // Inactive logs have no writers targeting them; no drain needed.
  back_fw->WaitDrain(back_A);

  IOStatus io_s;
  // Uniform per-log loop matching legacy's shape.  Per-log work:
  //   back log: FlushSlotBytes(back_A) + SyncWithoutFlush;
  //   inactive log: SyncWithoutFlush only.
  // On IO failure, §3.11 requires abort (see function header comment).
  for (log::Writer* log : logs_to_sync) {
    const bool is_back = (log->get_log_number() == current_log_number);
    if (is_back) {
      io_s = back_fw->FlushSlotBytes(back_A);
      if (!io_s.ok()) {
        ROCKS_LOG_FATAL(immutable_db_options_.info_log,
                        "SyncWAL FlushSlotBytes failed on log #%" PRIu64
                        ": %s. Aborting.",
                        log->get_log_number(), io_s.ToString().c_str());
        std::abort();
      }
    }
    io_s = log->file()->SyncWithoutFlush(immutable_db_options_.use_fsync);
    if (!io_s.ok()) {
      ROCKS_LOG_FATAL(immutable_db_options_.info_log,
                      "SyncWAL fsync failed on log #%" PRIu64
                      ": %s. Aborting.",
                      log->get_log_number(), io_s.ToString().c_str());
      std::abort();
    }
  }

  TEST_SYNC_POINT("DBImpl::SyncWALNoGroup:AfterFlushAndSync");

  if (need_log_dir_sync) {
    IOStatus dir_s = directories_.GetWalDir()->FsyncWithDirOptions(
        IOOptions(), nullptr,
        DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    if (!dir_s.ok()) {
      ROCKS_LOG_FATAL(immutable_db_options_.info_log,
                      "SyncWAL dir fsync failed: %s. Aborting.",
                      dir_s.ToString().c_str());
      std::abort();
    }
  }

  TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:1");
  VersionEdit synced_wals;
  {
    InstrumentedMutexLock l(&wal_write_mutex_);
    // Recycle A as the next standby BEFORE releasing the mutex, so the
    // next SyncWAL caller that wakes up from log_sync_cv_ sees slot
    // A->next_wal_offset == 0 (SwitchActive's precondition).
    back_fw->ResetDrainedSlot(back_A);
    // MarkLogsSynced walks logs_ up_to current_log_number.  Inactive
    // logs: pre_sync_size == GetFlushedSize (no bytes added between
    // PrepareForSync and here) → "fully synced" branch → erased.
    // Back log: pre_sync_size < GetFlushedSize (we added A's bytes
    // via FlushSlotBytes) → "Active WAL" branch → FinishSync only.
    MarkLogsSynced(current_log_number, need_log_dir_sync, &synced_wals);
    // Release the serialization flag last so queued callers wake into
    // a fully-consistent state: slot recycled, IsSyncing cleared on
    // every entry we touched, log_sync_cv_ signaled by MarkLogsSynced.
    sync_wal_in_progress_ = false;
    log_sync_cv_.SignalAll();
  }

  Status status;
  if (synced_wals.IsWalAddition()) {
    InstrumentedMutexLock l(&mutex_);
    status = ApplyWALToManifest(&synced_wals);
  }

  TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:2");
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
