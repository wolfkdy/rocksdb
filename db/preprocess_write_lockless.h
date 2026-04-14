//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;
struct WriteOptions;

// Reason returned by the lockless prewrite.  kOk means the writer may
// proceed to the §3.5 claim CAS.  Every other value routes to a slow path
// (structural switch, stall, or validation failure).  See
// eliminate_group_commit_design.md §3.8 / §3.9 / §4.5.
enum class PrewriteReason : uint8_t {
  kOk = 0,

  // Structural switch triggers (§3.9.1 — drain + mutex_).
  kDbStopped,
  kWalTooBig,
  kWbmShouldFlush,
  kTrimHistoryPending,
  kFlushPending,

  // SyncWAL serialization (§3.8 — wait on log_sync_cv_).
  kNeedLogSync,

  // Stall triggers (§3.9.2 — pure wait, no drain).
  kWriteStalled,
  kWbmShouldStall,

  // Batch validation (§4.5).
  kBatchTooLarge,
  kBatchCorrupt,
};

// Snapshot of the lockless condition sources read by PreprocessWrite.
// Every field is the value of an independent atomic/concurrent load —
// ordering among fields is unspecified.  The evaluator applies a fixed
// priority (same as DBImpl::PreprocessWrite) so two different orderings
// of the same set of true flags always produce the same reason.
struct PrewriteConditions {
  bool db_stopped = false;
  bool wal_too_big = false;
  bool wbm_should_flush = false;
  bool trim_history_pending = false;
  bool flush_pending = false;
  bool need_log_sync = false;
  bool write_stalled = false;     // write_controller IsStopped() || NeedsDelay()
  bool wbm_should_stall = false;
};

struct PrewriteResult {
  PrewriteReason reason = PrewriteReason::kOk;
  // Populated when reason == kBatchCorrupt: the decoded validation error.
  // Other reasons use the standard error string inferred from the reason.
  Status validation_status;
};

// Evaluates `cond` against the fixed priority ladder — returns the first
// triggered reason, or kOk if none fires.  The priority matches
// DBImpl::PreprocessWrite at db/db_impl/db_impl_write.cc:1134:
//   1. db_stopped  2. wal_too_big  3. wbm_should_flush
//   4. trim_history_pending  5. flush_pending
//   6. write_stalled  7. wbm_should_stall
//   8. need_log_sync
// (The legacy path handles need_log_sync last, after the stall waits, so
// we preserve that order.)
PrewriteReason EvaluatePrewriteConditions(const PrewriteConditions& cond);

// Batch-level validation (§4.5).  Returns:
//   kOk              — batch passes VerifyChecksum + fits the Link_buf ring
//   kBatchTooLarge   — batch_lsn_count is >= LinkBuf::kCapacity
//   kBatchCorrupt    — VerifyChecksum failed; `out_status` carries details
PrewriteResult ValidateWriteBatch(const WriteBatch* batch,
                                  uint64_t batch_lsn_count);

// Integration layer: reads the same condition sources as
// DBImpl::PreprocessWrite (db/db_impl/db_impl_write.cc:1134), with no
// locks, then runs the evaluator and batch validation.
//
// This function is not yet called by the write path.  It exists so the
// leaderless path can be exercised in isolation ahead of wiring.
PrewriteResult PreprocessWriteLockless(DBImpl* db,
                                       const WriteOptions& write_options,
                                       const WriteBatch* batch,
                                       uint64_t batch_lsn_count,
                                       bool need_log_sync);

}  // namespace ROCKSDB_NAMESPACE
