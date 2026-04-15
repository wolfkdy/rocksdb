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

Status DBImpl::SyncWALNoGroup() {
  TEST_SYNC_POINT("DBImpl::SyncWAL:Begin");

  assert(!immutable_db_options_.use_group_commit);
  assert(immutable_db_options_.nonblocking_wal_io);

  // Single CS under wal_write_mutex_:
  //  - if another switcher is already mid-sync (IsSyncing), wait for it
  //    to finish and return — its FinishSync covers our entry-time
  //    durability requirement;
  //  - otherwise, snapshot the log::Writer / file_writer / log_dir_synced_,
  //    publish B as the new active slot, and mark logs_.back() as syncing
  //    so concurrent SyncWAL callers take the wait branch above.
  // The mutex serializes claims (ClaimSlot also runs under it), so once
  // we publish B no new writer can land in A.  Existing in-flight
  // writers are drained outside the mutex by WaitDrain.
  log::Writer* log_writer;
  NonBlockingIOWritableFileWriter* file_writer;
  NonBlockingIOWritableFileWriter::BufferSlot* A;
  uint64_t current_log_number;
  bool need_log_dir_sync;
  {
    InstrumentedMutexLock l(&wal_write_mutex_);
    // Round-2 scope guard: structural switch path is not yet built so
    // logs_.size() is invariant 1.  Once structural switch lands, this
    // becomes a walk to find the entry to sync.
    assert(logs_.size() == 1);

    while (logs_.back().IsSyncing()) {
      log_sync_cv_.Wait();
    }
    // Re-check after wake: another switcher's FinishSync may have made
    // our entry-time bytes durable.  If logs_.back() advanced past us,
    // we'd return here — Round-2 has only one log so the check is moot.

    log_writer = logs_.back().writer;
    file_writer = static_cast<NonBlockingIOWritableFileWriter*>(
        log_writer->file());
    if (!log_writer->file()->writable_file()->IsSyncThreadSafe()) {
      return Status::NotSupported(
          "SyncWAL() is not supported for this implementation of WAL file",
          immutable_db_options_.allow_mmap_writes
              ? "try setting Options::allow_mmap_writes to false"
              : Slice());
    }
    current_log_number = log_writer->get_log_number();
    need_log_dir_sync = !log_dir_synced_;

    A = file_writer->ActiveSlot();
    NonBlockingIOWritableFileWriter::BufferSlot* B = file_writer->OtherSlot(A);
    file_writer->SwitchActive(A, B);
    logs_.back().PrepareForSync();
  }

  TEST_SYNC_POINT("DBImpl::SyncWALNoGroup:AfterPrepareForSync");
  RecordTick(stats_, WAL_FILE_SYNCED);

  // Drain in-flight writers that grabbed A before SwitchActive.  Done
  // without the mutex so writers can complete and ReleaseSlot.
  file_writer->WaitDrain(A);

  IOStatus io_s = file_writer->FlushSlotBytes(A);
  if (io_s.ok()) {
    io_s = file_writer->SyncWithoutFlush(immutable_db_options_.use_fsync);
  }
  TEST_SYNC_POINT("DBImpl::SyncWALNoGroup:AfterFlushAndSync");

  if (!io_s.ok()) {
    // §3.11: SyncWAL IO failure is unrecoverable.  B has already been
    // published, so returning an error would leave writers landing in a
    // buffer whose predecessor is in an undefined on-disk state.
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
    InstrumentedMutexLock l(&wal_write_mutex_);
    MarkLogsSynced(current_log_number, need_log_dir_sync, &synced_wals);
  }
  // Recycle A as the next standby.  Must happen after MarkLogsSynced
  // signals log_sync_cv_ so waiters wake into a fully-consistent state.
  file_writer->ResetDrainedSlot(A);

  if (synced_wals.IsWalAddition()) {
    InstrumentedMutexLock l(&mutex_);
    status = ApplyWALToManifest(&synced_wals);
  }

  TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:2");
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
