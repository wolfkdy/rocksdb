// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "file/nonblocking_io_writable_file_writer.h"

#include "port/port.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

void NonBlockingIOWritableFileWriter::SwapBuffer() {
  // Double-buffering is only for buffered I/O; direct I/O has its own path.
  assert(!use_direct_io());
  // WAL checksum handoff is rejected at DB open with nonblocking_wal_io.
  assert(!buffered_data_with_checksum_);
  // Don't swap into a broken writer — the previous flush failed.
  assert(!seen_error());
  // Swapping into a non-empty flush_buf_ would lose unflushed WAL data.
  if (flush_buf_.CurrentSize() != 0) {
    abort();
  }
  // Ping-pong: move buf_ -> flush_buf_ for writing; reuse flush_buf_'s old
  // allocation (now empty) as the new buf_ for incoming Append()s.
  std::swap(buf_, flush_buf_);
  // After swap, buf_ holds flush_buf_'s old (now-cleared) allocation, which
  // was initialized either in the constructor or by a previous swap cycle.
  assert(buf_.Capacity() > 0);
}

IOStatus NonBlockingIOWritableFileWriter::FlushSwappedBuffer(
    Env::IOPriority op_rate_limiter_priority) {
  if (seen_error()) {
    return AssertFalseAndGetStatusForPrevError();
  }
  // Double-buffering is only for buffered I/O; direct I/O has its own path.
  assert(!use_direct_io());
  // WAL checksum handoff is rejected at DB open with nonblocking_wal_io.
  assert(!buffered_data_with_checksum_);
  IOStatus s;

  if (flush_buf_.CurrentSize() > 0) {
    // filesize_ was already advanced by Append() when the data entered buf_
    // (before it was swapped into flush_buf_), so we must not update it again.
    // flushed_size_ is updated inside WriteToFile().
    assert(GetFlushedSize() + flush_buf_.CurrentSize() <= GetFileSize());
    // Use WriteToFile() rather than WriteBuffered() to avoid touching buf_,
    // which may be receiving new Append()s concurrently.
    s = WriteToFile(flush_buf_.BufferStart(), flush_buf_.CurrentSize(),
                    op_rate_limiter_priority);
    if (!s.ok()) {
      set_seen_error();
      flush_buf_.Clear();
      return s;
    }
  }
  flush_buf_.Clear();

  s = FlushOSCacheAndMaybeRangeSync(op_rate_limiter_priority);
  return s;
}

IOStatus NonBlockingIOWritableFileWriter::FlushSlotBytes(
    BufferSlot* slot, Env::IOPriority op_rate_limiter_priority) {
  assert(slot != nullptr);
  assert(!use_group_commit_);
  if (seen_error()) {
    return AssertFalseAndGetStatusForPrevError();
  }
  assert(!use_direct_io());
  assert(!buffered_data_with_checksum_);

  // Caller must be the switcher post-Phase-2, so next_wal_offset is stable.
  const uint64_t nbytes = slot->next_wal_offset;
  IOStatus s;
  if (nbytes > 0) {
    // Writers have already advanced filesize_ via AdvanceFileSize() for
    // every byte we're about to write, so flushed_size_ + nbytes must be
    // within filesize_.
    assert(GetFlushedSize() + nbytes <= GetFileSize());
    const char* src = (slot == &slots_[0]) ? buf_.BufferStart()
                                            : flush_buf_.BufferStart();
    s = WriteToFile(src, static_cast<size_t>(nbytes),
                    op_rate_limiter_priority);
    if (!s.ok()) {
      set_seen_error();
      return s;
    }
  }
  s = FlushOSCacheAndMaybeRangeSync(op_rate_limiter_priority);
  return s;
}

NonBlockingIOWritableFileWriter::BufferSlot*
NonBlockingIOWritableFileWriter::ClaimSlot() {
  // §3.5.2: loop until the single atomic CAS that simultaneously verifies
  // state == ACTIVE and increments count succeeds.  While state is
  // PENDING (switch in progress), spin via cpu_relax until the switcher
  // publishes a new active slot.
  for (;;) {
    BufferSlot* A = active_buffer_ptr_.load(std::memory_order_acquire);
    uint64_t h = A->header.load(std::memory_order_acquire);
    if (HeaderState(h) != kBufferActive) {
      port::AsmVolatilePause();
      continue;
    }
    uint64_t new_h = MakeHeader(kBufferActive, HeaderCount(h) + 1);
    if (A->header.compare_exchange_weak(h, new_h,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
      return A;
    }
    // CAS failed — either count changed (another writer claimed) or state
    // changed (switcher closed A between load and CAS).  Retry the outer
    // loop, which re-reads active_buffer_ptr_ and A.header.
  }
}

void NonBlockingIOWritableFileWriter::ReleaseSlot(BufferSlot* slot) {
  assert(slot != nullptr);
  // §3.5.3: fetch_sub(1) decrements the low 48 bits only; count is always
  // >= 1 when this is called so the subtraction does not borrow into the
  // state bits at 48..49.
  uint64_t prev = slot->header.fetch_sub(1, std::memory_order_release);
  assert(HeaderCount(prev) >= 1);
  (void)prev;
}

NonBlockingIOWritableFileWriter::SwitchResult
NonBlockingIOWritableFileWriter::CloseDrainOpen(BufferSlot* A, BufferSlot* B) {
  assert(A != nullptr && B != nullptr && A != B);

  // ----- Phase 1: close A (CAS state ACTIVE -> PENDING) -----
  for (;;) {
    uint64_t h = A->header.load(std::memory_order_acquire);
    if (HeaderState(h) != kBufferActive) {
      // Lost the race: another switcher already closed A.  Wait for that
      // switcher's Phase 3 (active_buffer_ptr_ publishes B), then return.
      while (active_buffer_ptr_.load(std::memory_order_acquire) == A) {
        port::AsmVolatilePause();
      }
      return SwitchResult::kNotSwitcher;
    }
    uint64_t new_h = MakeHeader(kBufferPending, HeaderCount(h));
    if (A->header.compare_exchange_weak(h, new_h,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
      break;
    }
    // CAS failed — either count changed (another writer just claimed in A)
    // or state changed (another switcher raced in).  The next iteration's
    // state check handles the latter.
  }

  // ----- Phase 2: drain A (wait for count to reach 0) -----
  while (HeaderCount(A->header.load(std::memory_order_acquire)) != 0) {
    port::AsmVolatilePause();
  }

  // ----- Phase 3: prepare B and publish it as the new active slot -----
  // Callers that need to do slow structural work (SwitchWAL /
  // SwitchMemtable / ...) do so between Phase 2 and Phase 3.  In Round 1
  // there are no such callers; the test-facing primitive just publishes B.
  //
  // B must be FREE with count 0 here.  If not, the A/B pairing logic
  // upstream is wrong — no writer should have a slot in B, because B was
  // the standby.
  const uint64_t b_h = B->header.load(std::memory_order_acquire);
  assert(HeaderState(b_h) == kBufferFree);
  assert(HeaderCount(b_h) == 0);
  (void)b_h;
  // B's scratch region starts at the absolute file offset immediately
  // after A's highest reserved byte.  A->next_wal_offset was frozen by
  // Phase 2's drain — no writer may advance it after count reached 0.
  // SerializeRecord uses (B->file_offset_start + writer_offset) to
  // compute log-record block padding (§3.10).
  B->next_wal_offset = 0;
  B->file_offset_start = A->file_offset_start + A->next_wal_offset;
  B->header.store(MakeHeader(kBufferActive, 0), std::memory_order_release);
  active_buffer_ptr_.store(B, std::memory_order_release);
  return SwitchResult::kSwitcher;
}

void NonBlockingIOWritableFileWriter::MarkBufferFree(BufferSlot* slot) {
  assert(slot != nullptr);
  // Caller must have been the switcher that closed this slot.  Header
  // must currently be {PENDING, 0}.
  const uint64_t h = slot->header.load(std::memory_order_acquire);
  assert(HeaderState(h) == kBufferPending);
  assert(HeaderCount(h) == 0);
  (void)h;
  slot->next_wal_offset = 0;
  slot->header.store(MakeHeader(kBufferFree, 0), std::memory_order_release);
}

}  // namespace ROCKSDB_NAMESPACE
