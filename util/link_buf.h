// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Circular ring that tracks which LSN ranges have been completed by writers,
// so that a global `min_non_hole_lsn` watermark can be advanced lock-free.
//
// Used by the eliminate_group_commit write path (see
// eliminate_group_commit_design.md §3.6).
//
// Model:
//   - Each slot at index (lsn % kCapacity) stores the end-LSN of a completed
//     batch that starts at `lsn`.  A slot value of 0 means "no completed
//     batch starts here" (hole).
//   - `Store(start, end)` marks the batch [start, end) complete.
//   - `AdvanceWatermark()` opportunistically walks the chain from the current
//     watermark forward, one batch per hop, until it hits a hole.
//   - Monotonically non-decreasing watermark.  Lock-free; concurrent stores
//     and advances are safe.
class LinkBuf {
 public:
  // Hard cap on the number of outstanding in-flight LSNs this structure can
  // represent.  Sized to cover max_concurrent_writers * max_batch_lsn_count
  // with generous headroom.  Changing this value changes the wire-level
  // resource footprint; kept as a compile-time constant.
  static constexpr size_t kCapacity = 64 * 1024;

  LinkBuf();

  // Not copyable / movable — the atomics pin this to a fixed address.
  LinkBuf(const LinkBuf&) = delete;
  LinkBuf& operator=(const LinkBuf&) = delete;

  // Initializes the watermark to `lsn`.  Must be called before any Store() or
  // AdvanceWatermark() when starting a fresh DB or after recovery.
  void Reset(uint64_t lsn);

  // Records that the batch [start_lsn, end_lsn) has completed memtable insert
  // and WAL memcpy.  Must be called exactly once per batch, with end_lsn !=
  // 0 (i.e., a nonzero batch size) and end_lsn > start_lsn.
  void Store(uint64_t start_lsn, uint64_t end_lsn);

  // Opportunistically advances the watermark.  Returns the new watermark.
  // Safe to call concurrently from multiple threads; the CAS serializes them
  // and losers simply give up without retrying.
  uint64_t AdvanceWatermark();

  // Current watermark.  All LSNs strictly below this value have completed.
  uint64_t MinNonHoleLsn() const {
    return min_non_hole_lsn_.load(std::memory_order_acquire);
  }

  // Prewrite-time check: a single batch that needs >= kCapacity LSNs cannot
  // fit in the ring.  Returns Status::Busy if the batch is too large; the
  // caller should fall back to the slow path.
  static Status CheckBatchSize(uint64_t batch_lsn_count);

  // Post-pair-advance check: cumulative in-flight LSN range must stay below
  // kCapacity, or the ring would wrap and collide.  Aborts the process on
  // violation — this fires only if a writer has been stuck for an
  // extraordinary duration (see §3.6.4 check 2 + §3.11 error policy).
  void CheckRingCapacityOrAbort(uint64_t start_lsn,
                                uint64_t batch_lsn_count) const;

 private:
  std::atomic<uint64_t> links_[kCapacity];
  std::atomic<uint64_t> min_non_hole_lsn_;
};

}  // namespace ROCKSDB_NAMESPACE
