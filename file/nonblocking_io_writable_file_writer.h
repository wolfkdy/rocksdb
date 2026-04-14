// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstdint>

#include "file/writable_file_writer.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {

// NonBlockingIOWritableFileWriter extends WritableFileWriter for use with
// the nonblocking_wal_io mode (manual_wal_flush=false, two_write_queues=false).
//
// It suppresses all implicit I/O during Append() and provides double-buffering
// so SyncWAL() can swap the write buffer under log_write_mutex_, then write to
// disk and fsync without holding it.
//
// Double-buffering protocol:
//   1. Call SwapBuffer() while holding log_write_mutex_:
//      moves buf_ -> flush_buf_, resets buf_ for new Append()s.
//   2. Call FlushSwappedBuffer() WITHOUT holding log_write_mutex_:
//      writes flush_buf_ to the OS and issues Flush(); does not touch buf_.
//
// The caller (AddRecord via WriteToWAL) must hold log_write_mutex_ during
// Append() to serialize with the buffer swap in SyncWAL().
//
// Slot / close-drain-open primitives (eliminate_group_commit path)
// ----------------------------------------------------------------
// In addition to the buf_/flush_buf_ swap protocol above (used by the
// existing WriteThread-leader path with nonblocking_wal_io=true), this class
// carries the per-buffer packed header and next_wal_offset that the new
// leaderless write path uses.  See eliminate_group_commit_design.md §3.5
// for the full specification.
//
// These new fields are a pure addition: they are initialized to benign
// values (slot 0 ACTIVE, slot 1 FREE, counts and offsets zero) and are not
// read or written by the existing SwapBuffer() / FlushSwappedBuffer() code
// paths.  They only move when the new primitives (ClaimSlot, ReleaseSlot,
// CloseDrainOpen) are invoked — which happens exclusively in the new write
// path, gated by the eliminate_group_commit feature flag.
class NonBlockingIOWritableFileWriter : public WritableFileWriter {
 public:
  // Per-buffer slot carrying the §3.5.1 packed header and the
  // per-buffer next_wal_offset.
  //
  // Header layout (bits 0..63 of header_):
  //   bits  0..47  in_flight_count
  //   bits 48..49  state (see BufferState)
  //   bits 50..63  reserved (zero)
  struct BufferSlot {
    std::atomic<uint64_t> header;
    uint64_t next_wal_offset;
  };

  enum BufferState : uint64_t {
    kBufferFree = 0,
    kBufferActive = 1,
    kBufferPending = 2,
  };

  enum class SwitchResult {
    // Caller performed Phase-1 CAS; they own A and must flush it, then
    // transition A back to FREE via MarkBufferFree().
    kSwitcher,
    // Another switcher raced in first and has already published B.
    kNotSwitcher,
  };

  // Header packing helpers.  Public so tests and Round-2 call sites can
  // reason about header state without re-deriving the bit layout.
  static constexpr uint64_t kHeaderCountBits = 48;
  static constexpr uint64_t kHeaderCountMask =
      (uint64_t{1} << kHeaderCountBits) - 1;

  static uint64_t MakeHeader(uint64_t state, uint64_t count) {
    // Reserved bits (50..63) must stay zero; callers must not pack anything
    // into them.
    return (state << kHeaderCountBits) | (count & kHeaderCountMask);
  }
  static uint64_t HeaderState(uint64_t h) {
    return (h >> kHeaderCountBits) & 0x3;
  }
  static uint64_t HeaderCount(uint64_t h) { return h & kHeaderCountMask; }

  // checksum handoff (perform_data_verification / buffered_data_with_checksum)
  // is rejected at DB open when nonblocking_wal_io is enabled, so those
  // parameters are omitted here.
  NonBlockingIOWritableFileWriter(
      std::unique_ptr<FSWritableFile>&& file, const std::string& file_name,
      const FileOptions& options, SystemClock* clock = nullptr,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr,
      Statistics* stats = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {})
      : WritableFileWriter(std::move(file), file_name, options, clock,
                           io_tracer, stats, listeners,
                           /*file_checksum_gen_factory=*/nullptr,
                           /*perform_data_verification=*/false,
                           /*buffered_data_with_checksum=*/false) {
    // Pre-allocate flush_buf_ so that after the first SwapBuffer() the
    // swapped-in buf_ already has a usable allocation.
    flush_buf_.Alignment(writable_file()->GetRequiredBufferAlignment());
    flush_buf_.AllocateNewBuffer(
        std::min((size_t)65536, max_buffer_size_));

    // Initialize the slot machinery: slots_[0] is ACTIVE on construction,
    // slots_[1] is FREE.  active_buffer_ptr_ names the current ACTIVE slot.
    slots_[0].header.store(MakeHeader(kBufferActive, 0),
                           std::memory_order_relaxed);
    slots_[0].next_wal_offset = 0;
    slots_[1].header.store(MakeHeader(kBufferFree, 0),
                           std::memory_order_relaxed);
    slots_[1].next_wal_offset = 0;
    active_buffer_ptr_.store(&slots_[0], std::memory_order_relaxed);
  }

  // Swap buf_ into flush_buf_ so that FlushSwappedBuffer() can write it to
  // disk without holding the mutex while Append()s continue into the fresh
  // buf_. Must be called under log_write_mutex_.
  void SwapBuffer();

  // Write flush_buf_ to the OS and issue Flush(). Safe to call without any
  // mutex because it does not touch buf_. Must follow SwapBuffer().
  IOStatus FlushSwappedBuffer(
      Env::IOPriority op_rate_limiter_priority = Env::IO_TOTAL);

  // Returns true when flush_buf_ is empty, i.e. no SwapBuffer() is pending
  // flush.  Useful for invariant checks by callers holding log_write_mutex_.
  bool IsFlushBufEmpty() const { return flush_buf_.CurrentSize() == 0; }

  // ---- Slot primitives for the eliminate_group_commit write path ----
  //
  // These methods are completely independent of SwapBuffer() /
  // FlushSwappedBuffer() above.  They coordinate the leaderless write path
  // via a pair of BufferSlot objects; see §3.5 of the design doc.

  // Returns the currently active slot.  Plain read — provided for tests and
  // for callers that need to pick an (A, B) pair for CloseDrainOpen().
  BufferSlot* ActiveSlot() {
    return active_buffer_ptr_.load(std::memory_order_acquire);
  }
  // The "other" slot — useful for selecting B given A.
  BufferSlot* OtherSlot(BufferSlot* s) {
    return (s == &slots_[0]) ? &slots_[1] : &slots_[0];
  }
  BufferSlot* Slot(size_t i) { return &slots_[i]; }

  // §3.5.2 Writer claim protocol.  Increments the in-flight count on the
  // active slot via a single CAS that simultaneously verifies state ==
  // ACTIVE.  Spins via cpu_relax while a switch is in progress.  Returns
  // the slot in which the caller now holds an in-flight reservation; the
  // caller must later call ReleaseSlot() on the same pointer.
  BufferSlot* ClaimSlot();

  // §3.5.3 Writer release protocol.  Decrements the in-flight count;
  // state bits are unaffected.
  void ReleaseSlot(BufferSlot* slot);

  // §3.5.4 Close-drain-open primitive.  A is the slot we want to close;
  // B is the other slot (must be FREE when Phase 3 runs).
  //
  // Returns kSwitcher if this thread's Phase-1 CAS succeeded.  The caller
  // then owns A: they must flush A's bytes and, when done, call
  // MarkBufferFree(A) to recycle A as the next standby.
  //
  // Returns kNotSwitcher if another switcher was already past Phase 1 on
  // A.  In that case this method blocks until active_buffer_ptr_ moves
  // off A (i.e., the winning switcher reached Phase 3), then returns.
  // The caller does not own A.
  SwitchResult CloseDrainOpen(BufferSlot* A, BufferSlot* B);

  // Recycle a slot that a switcher has finished flushing.  Stores
  // {state: FREE, count: 0} and resets next_wal_offset.  Must only be
  // called by the switcher that previously got kSwitcher from
  // CloseDrainOpen() on this slot.
  void MarkBufferFree(BufferSlot* slot);

 protected:
#ifndef NDEBUG
  // WriteBuffered is called by Flush() (via WriteBuffer) to drain buf_ to
  // the OS.  Assert that flush_buf_ has already been drained — otherwise
  // we'd have two concurrent writes to the same underlying file.
  IOStatus WriteBuffered(const char* data, size_t size,
                         Env::IOPriority op_rate_limiter_priority) override {
    assert(flush_buf_.CurrentSize() == 0);
    return WritableFileWriter::WriteBuffered(data, size,
                                            op_rate_limiter_priority);
  }

  // WAL checksum handoff is incompatible with nonblocking_wal_io (rejected
  // at DB open).  Assert this path is never reached.
  IOStatus WriteBufferedWithChecksum(
      const char* /*data*/, size_t /*size*/,
      Env::IOPriority /*op_rate_limiter_priority*/) override {
    assert(false);
    return IOStatus::NotSupported(
        "WriteBufferedWithChecksum not supported with nonblocking_wal_io");
  }
#endif

  // Allow buf_ to grow without bound so that Append() never triggers
  // implicit I/O. SyncWAL() is responsible for draining the buffer.
  size_t AppendBufferSizeLimit() const override { return size_t(-1); }

  // Never flush implicitly during Append(); the caller is responsible for
  // explicitly calling SwapBuffer() + FlushSwappedBuffer() via SyncWAL().
  bool ShouldImplicitFlushOnAppend() const override { return false; }

 private:
  // Second buffer for ping-pong double-buffering.
  // Non-empty only between SwapBuffer() and FlushSwappedBuffer() completing.
  AlignedBuffer flush_buf_;

  // Two persistent slots for the eliminate_group_commit path.  These do
  // NOT alias buf_ / flush_buf_; they are independent state used only by
  // the new write path (see §3.5).  On construction slots_[0] is ACTIVE
  // and slots_[1] is FREE.
  BufferSlot slots_[2];
  std::atomic<BufferSlot*> active_buffer_ptr_;
};

}  // namespace ROCKSDB_NAMESPACE
