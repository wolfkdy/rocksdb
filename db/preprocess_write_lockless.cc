//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/preprocess_write_lockless.h"

#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/flush_scheduler.h"
#include "db/trim_history_scheduler.h"
#include "db/write_controller.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "util/link_buf.h"

namespace ROCKSDB_NAMESPACE {

PrewriteReason EvaluatePrewriteConditions(const PrewriteConditions& cond) {
  if (cond.db_stopped) return PrewriteReason::kDbStopped;
  if (cond.wal_too_big) return PrewriteReason::kWalTooBig;
  if (cond.wbm_should_flush) return PrewriteReason::kWbmShouldFlush;
  if (cond.trim_history_pending) return PrewriteReason::kTrimHistoryPending;
  if (cond.flush_pending) return PrewriteReason::kFlushPending;
  if (cond.write_stalled) return PrewriteReason::kWriteStalled;
  if (cond.wbm_should_stall) return PrewriteReason::kWbmShouldStall;
  if (cond.need_log_sync) return PrewriteReason::kNeedLogSync;
  return PrewriteReason::kOk;
}

PrewriteResult ValidateWriteBatch(const WriteBatch* batch,
                                  uint64_t batch_lsn_count) {
  PrewriteResult res;
  if (batch == nullptr) {
    return res;
  }
  Status cap = LinkBuf::CheckBatchSize(batch_lsn_count);
  if (!cap.ok()) {
    res.reason = PrewriteReason::kBatchTooLarge;
    res.validation_status = cap;
    return res;
  }
  Status cs = batch->VerifyChecksum();
  if (!cs.ok()) {
    res.reason = PrewriteReason::kBatchCorrupt;
    res.validation_status = cs;
    return res;
  }
  return res;
}

PrewriteResult PreprocessWriteLockless(DBImpl* db,
                                       const WriteOptions& /*write_options*/,
                                       const WriteBatch* batch,
                                       uint64_t batch_lsn_count,
                                       bool need_log_sync) {
  PrewriteResult batch_res = ValidateWriteBatch(batch, batch_lsn_count);
  if (batch_res.reason != PrewriteReason::kOk) {
    return batch_res;
  }

  PrewriteConditions cond;
  cond.db_stopped = db->error_handler_.IsDBStopped();
  // wal_too_big intentionally left at its default (false) for the
  // leaderless write path.  Rationale: total_log_size_ does NOT drop
  // when SwitchWAL() runs — it only drops when the old WAL is actually
  // deleted by background flush + file-deletion, which can lag by 10s
  // of ms to seconds.  Returning kWalTooBig from prewrite would either
  // (a) livelock a retry loop (every retry sees the same condition),
  // or (b) force one-shot semantics that diverge from the other
  // structural reasons.  Instead, StructuralSwitchNoGroup re-checks
  // total_log_size_ and piggy-backs SwitchWAL when entered for some
  // other (self-resolving) reason.  The cost is that a DB whose ONLY
  // slow-path signal is wal_too_big (e.g. many CFs pinning small
  // immutable memtables) won't autonomously roll its oldest WAL —
  // legacy PreprocessWrite does, through repeated no-op SwitchWAL
  // calls.  Accepted for now; revisit alongside the Round-3 stall /
  // backpressure work (eliminate_group_commit impl plan 2.5+).
  cond.wbm_should_flush =
      db->write_buffer_manager_ != nullptr &&
      db->write_buffer_manager_->ShouldFlush();
  cond.trim_history_pending = !db->trim_history_scheduler_.Empty();
  cond.flush_pending = !db->flush_scheduler_.Empty();
  cond.write_stalled =
      db->write_controller_.IsStopped() || db->write_controller_.NeedsDelay();
  cond.wbm_should_stall =
      db->write_buffer_manager_ != nullptr &&
      db->write_buffer_manager_->ShouldStall();
  cond.need_log_sync = need_log_sync;

  PrewriteResult res;
  res.reason = EvaluatePrewriteConditions(cond);
  return res;
}

}  // namespace ROCKSDB_NAMESPACE
