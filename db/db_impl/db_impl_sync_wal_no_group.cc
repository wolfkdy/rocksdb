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
#include "monitoring/statistics.h"
#include "rocksdb/io_status.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

Status DBImpl::SyncWALNoGroup() {
  TEST_SYNC_POINT("DBImpl::SyncWAL:Begin");

  assert(!immutable_db_options_.use_group_commit);
  assert(immutable_db_options_.nonblocking_wal_io);

  // Snapshot the current log under log_write_mutex_.  For 2.2 scope there
  // is no structural switch path yet, so logs_.size() == 1 is invariant
  // (same Round-2 scope guard as WriteImplNoGroup — lifted when 2.3 lands).
  log::Writer* log_writer;
  NonBlockingIOWritableFileWriter* file_writer;
  uint64_t current_log_number;
  bool need_log_dir_sync;
  {
    InstrumentedMutexLock l(&log_write_mutex_);
    assert(logs_.size() == 1);
    auto& log = logs_.back();
    log_writer = log.writer;
    file_writer =
        static_cast<NonBlockingIOWritableFileWriter*>(log_writer->file());
    current_log_number = logfile_number_;
    if (!log_writer->file()->writable_file()->IsSyncThreadSafe()) {
      return Status::NotSupported(
          "SyncWAL() is not supported for this implementation of WAL file",
          immutable_db_options_.allow_mmap_writes
              ? "try setting Options::allow_mmap_writes to false"
              : Slice());
    }
    need_log_dir_sync = !log_dir_synced_;
  }

  // Race with other switchers (concurrent SyncWAL callers, and — once 2.3
  // lands — structural switchers) via CloseDrainOpen's Phase-1 CAS.
  NonBlockingIOWritableFileWriter::BufferSlot* A = file_writer->ActiveSlot();
  NonBlockingIOWritableFileWriter::BufferSlot* B = file_writer->OtherSlot(A);
  auto res = file_writer->CloseDrainOpen(A, B);

  if (res == NonBlockingIOWritableFileWriter::SwitchResult::kNotSwitcher) {
    // Another switcher already closed A.  Our durability requirement
    // (everything visible at our entry must be on disk before we return)
    // is satisfied as soon as the winner's FinishSync fires — it is the
    // winner's responsibility to flush A's bytes and fsync.  Wait on
    // log_sync_cv_ until IsSyncing() clears on this log (§3.4 + §4.4
    // Case 4).  Directory sync is owned by the winner on the same path.
    InstrumentedMutexLock l(&log_write_mutex_);
    // logs_.size() == 1 in 2.2 scope — guard it (see 2.3 for the
    // logs-iteration form that mirrors legacy SyncWAL).
    assert(logs_.size() == 1);
    while (logs_.back().IsSyncing()) {
      log_sync_cv_.Wait();
    }
    TEST_SYNC_POINT("DBImpl::SyncWALNoGroup:NotSwitcherReturn");
    return Status::OK();
  }

  // We are the switcher.  CloseDrainOpen has already published B as the
  // active slot — new writers are landing in B.  Flush A's bytes and
  // fsync; concurrent SyncWAL losers wait on log_sync_cv_.
  {
    InstrumentedMutexLock l(&log_write_mutex_);
    assert(logs_.size() == 1);
    logs_.back().PrepareForSync();
  }

  TEST_SYNC_POINT("DBImpl::SyncWALNoGroup:AfterPrepareForSync");
  RecordTick(stats_, WAL_FILE_SYNCED);

  IOStatus io_s = file_writer->FlushSlotBytes(A);
  if (io_s.ok()) {
    io_s = file_writer->SyncWithoutFlush(immutable_db_options_.use_fsync);
  }
  TEST_SYNC_POINT("DBImpl::SyncWALNoGroup:AfterFlushAndSync");

  if (!io_s.ok()) {
    // §3.11: SyncWAL IO failure is unrecoverable.  Phase 3 has already
    // published B, so returning an error would leave writers landing in
    // a buffer whose predecessor is in an undefined on-disk state.
    ROCKS_LOG_FATAL(immutable_db_options_.info_log,
                    "SyncWAL flush/fsync failed: %s. Aborting.",
                    io_s.ToString().c_str());
    std::abort();
  }

  Status status;
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
    InstrumentedMutexLock l(&log_write_mutex_);
    MarkLogsSynced(current_log_number, need_log_dir_sync, &synced_wals);
  }
  // Recycle A as the next standby.  Must happen after MarkLogsSynced has
  // cleared IsSyncing + signalled waiters so losers wake into a
  // fully-consistent state; the FREE transition itself is independent of
  // the sync flag and ordering between the two is loose.
  file_writer->MarkBufferFree(A);

  if (synced_wals.IsWalAddition()) {
    InstrumentedMutexLock l(&mutex_);
    status = ApplyWALToManifest(&synced_wals);
  }

  TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:2");
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
