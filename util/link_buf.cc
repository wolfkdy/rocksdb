// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "util/link_buf.h"

#include <cstdlib>

namespace ROCKSDB_NAMESPACE {

LinkBuf::LinkBuf() : min_non_hole_lsn_(0) {
  for (size_t i = 0; i < kCapacity; ++i) {
    links_[i].store(0, std::memory_order_relaxed);
  }
}

void LinkBuf::Reset(uint64_t lsn) {
  for (size_t i = 0; i < kCapacity; ++i) {
    links_[i].store(0, std::memory_order_relaxed);
  }
  min_non_hole_lsn_.store(lsn, std::memory_order_release);
}

void LinkBuf::Store(uint64_t start_lsn, uint64_t end_lsn) {
  assert(end_lsn > start_lsn);
  // end_lsn == 0 is the sentinel for "empty slot"; a real batch must have
  // end_lsn > start_lsn >= 0, so end_lsn is always > 0.
  links_[start_lsn % kCapacity].store(end_lsn, std::memory_order_release);
}

uint64_t LinkBuf::AdvanceWatermark() {
  for (;;) {
    uint64_t pos = min_non_hole_lsn_.load(std::memory_order_acquire);
    uint64_t next = links_[pos % kCapacity].load(std::memory_order_acquire);
    if (next <= pos) {
      // Hole: either the slot was never stored (value 0) or it holds a
      // stale end_lsn from a prior wrap that is <= the current watermark.
      // Both cases are indistinguishable and both mean "no completed batch
      // starts at pos yet".
      return pos;
    }
    // Wrap-past guard (mirrors MySQL Link_buf's `next_load >= position +
    // m_capacity` check).  If this fires, a writer has stored into a slot
    // whose end-LSN is more than kCapacity ahead of the watermark, meaning
    // the ring has wrapped by more than one generation — CheckRingCapacity
    // -OrAbort should have aborted before this ever happens.
    assert(next < pos + kCapacity);
    if (min_non_hole_lsn_.compare_exchange_weak(
            pos, next, std::memory_order_acq_rel, std::memory_order_acquire)) {
      // Intentionally do NOT clear the slot.  Clearing would race with a
      // concurrent writer whose start_lsn hashes back to this slot (which
      // is legal as soon as watermark has advanced enough to pass
      // CheckRingCapacityOrAbort — well before kCapacity LSNs have elapsed).
      // Stale values are filtered by the `next <= pos` check above, and
      // fresh stores at the same index just overwrite the stale value.
      continue;
    }
    // Lost the race: another thread is advancing.  Give up.
    return min_non_hole_lsn_.load(std::memory_order_acquire);
  }
}

Status LinkBuf::CheckBatchSize(uint64_t batch_lsn_count) {
  if (batch_lsn_count >= kCapacity) {
    return Status::Busy("WriteBatch too large for link buffer");
  }
  return Status::OK();
}

void LinkBuf::CheckRingCapacityOrAbort(uint64_t start_lsn,
                                       uint64_t batch_lsn_count) const {
  uint64_t watermark = min_non_hole_lsn_.load(std::memory_order_acquire);
  // Cumulative in-flight range: [watermark, start_lsn + batch_lsn_count).
  // If this exceeds kCapacity the ring wraps and slots collide — catastrophic.
  if (start_lsn + batch_lsn_count - watermark >= kCapacity) {
    std::abort();
  }
}

}  // namespace ROCKSDB_NAMESPACE
