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

  // Caller must be the switcher post-WaitDrain, so next_wal_offset is stable.
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
  // Caller holds wal_write_mutex_, which serializes against SwitchActive,
  // so the active pointer cannot change under us.  Relaxed bumps are
  // sufficient — pairing with WaitDrain's acquire load happens via the
  // matching ReleaseSlot's release decrement.
  BufferSlot* s = active_buffer_ptr_.load(std::memory_order_relaxed);
  s->in_flight_count.fetch_add(1, std::memory_order_relaxed);
  return s;
}

void NonBlockingIOWritableFileWriter::ReleaseSlot(BufferSlot* slot) {
  assert(slot != nullptr);
  // Release ordering so the slot bytes the writer wrote (and the
  // AdvanceFileSize bump) happen-before WaitDrain's acquire load sees
  // count reach zero.
  uint64_t prev = slot->in_flight_count.fetch_sub(1, std::memory_order_release);
  assert(prev >= 1);
  (void)prev;
}

void NonBlockingIOWritableFileWriter::SwitchActive(BufferSlot* A,
                                                   BufferSlot* B) {
  assert(A != nullptr && B != nullptr && A != B);
  // Caller must hold wal_write_mutex_.  Under that lock, A is the
  // current active and B has been drained (WaitDrain'd by the previous
  // switcher and ResetDrainedSlot).
  assert(active_buffer_ptr_.load(std::memory_order_relaxed) == A);
  assert(B->in_flight_count.load(std::memory_order_relaxed) == 0);
  assert(B->next_wal_offset == 0);

  // B's scratch region begins at the byte after A's last reserved byte.
  // SerializeRecord uses `(B->file_offset_start + writer_offset)` for
  // log-record block padding (§3.10).
  B->file_offset_start = A->file_offset_start + A->next_wal_offset;
  // Release-store so any thread that reads active_buffer_ptr_ outside
  // the mutex (e.g. ActiveSlot() in tests) observes the offset writes.
  active_buffer_ptr_.store(B, std::memory_order_release);
}

void NonBlockingIOWritableFileWriter::WaitDrain(BufferSlot* slot) {
  assert(slot != nullptr);
  // Switcher spins for in-flight writers (those between ClaimSlot and
  // ReleaseSlot at the moment of SwitchActive) to finish.  Acquire load
  // pairs with ReleaseSlot's release dec.
  while (slot->in_flight_count.load(std::memory_order_acquire) != 0) {
    port::AsmVolatilePause();
  }
}

void NonBlockingIOWritableFileWriter::ResetDrainedSlot(BufferSlot* slot) {
  assert(slot != nullptr);
  assert(slot->in_flight_count.load(std::memory_order_relaxed) == 0);
  slot->next_wal_offset = 0;
}

}  // namespace ROCKSDB_NAMESPACE
