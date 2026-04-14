// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

// Paired (next_lsn, next_wal_offset) advance under a single spinlock.  Used
// by the eliminate_group_commit write path (see
// eliminate_group_commit_design.md §3.2).
//
// The critical section is four plain stores and two plain loads:
//
//   spinlock_acquire(mu_)
//     lsn                = next_lsn_
//     offset             = *next_wal_offset
//     next_lsn_         += lsn_count
//     *next_wal_offset  += byte_count
//   spinlock_release(mu_)
//
// Pairing the two advances under a single critical section is what makes the
// partial-order invariant (§3.2.3) hold inside a single active buffer: for
// any two writers i, j in the same buffer, `lsn[i] < lsn[j] ⇔ off[i] <
// off[j]`.  Two independent fetch_adds would not guarantee this.
//
// The per-buffer `next_wal_offset` is owned by the buffer, not by this
// allocator.  The caller passes a pointer to the active buffer's offset
// field; across a buffer switch the caller will pass the other buffer's
// offset pointer.  The switcher (§3.5.4) guarantees that `next_wal_offset`
// is stable for the duration of the spinlock body — see §3.2.2.
class LsnOffsetAllocator {
 public:
  struct Pair {
    uint64_t start_lsn;
    uint64_t start_offset;
  };

  // Constructs an allocator with `next_lsn_` initialized to `initial_lsn`.
  explicit LsnOffsetAllocator(uint64_t initial_lsn = 0)
      : next_lsn_(initial_lsn) {}

  LsnOffsetAllocator(const LsnOffsetAllocator&) = delete;
  LsnOffsetAllocator& operator=(const LsnOffsetAllocator&) = delete;

  // Atomically advances `next_lsn_` by `lsn_count` and `*next_wal_offset` by
  // `byte_count`, returning the pair of values observed immediately before
  // the advance.  `lsn_count` and `byte_count` may be zero (used by the
  // stall path to peek without advancing — callers should generally prefer
  // PeekPair in that case).
  Pair Advance(uint64_t lsn_count, uint64_t byte_count,
               uint64_t* next_wal_offset);

  // Reads both values atomically without advancing either.  Included for
  // tests and for assertions in the write path; production code should
  // generally call Advance.
  Pair PeekPair(uint64_t* next_wal_offset) const;

  // Reset for recovery.  Must not race with Advance.
  void Reset(uint64_t new_next_lsn) { next_lsn_ = new_next_lsn; }

 private:
  mutable SpinMutex mu_;
  uint64_t next_lsn_;
};

}  // namespace ROCKSDB_NAMESPACE
