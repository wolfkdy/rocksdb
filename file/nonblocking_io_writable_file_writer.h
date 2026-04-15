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
// so SyncWAL() can swap the write buffer under wal_write_mutex_, then write to
// disk and fsync without holding it.
//
// Double-buffering protocol (legacy WriteThread-leader path):
//   1. Call SwapBuffer() while holding wal_write_mutex_:
//      moves buf_ -> flush_buf_, resets buf_ for new Append()s.
//   2. Call FlushSwappedBuffer() WITHOUT holding wal_write_mutex_:
//      writes flush_buf_ to the OS and issues Flush(); does not touch buf_.
//
// Slot primitives (eliminate_group_commit / leaderless write path)
// ----------------------------------------------------------------
// In addition to the buf_/flush_buf_ swap protocol above, this class
// carries two BufferSlots used by the leaderless write path.  See
// eliminate_group_commit_design.md §3.5.
//
// All claim/switch decisions are serialized by the DB-level
// wal_write_mutex_, so the slot machinery uses a plain in_flight_count
// (no packed state bits) and a relaxed fetch_add for ClaimSlot.  The
// switcher's drain (WaitDrain) is the only step performed without the
// mutex held.
class NonBlockingIOWritableFileWriter : public WritableFileWriter {
 public:
  // Per-buffer slot.
  //
  // file_offset_start is the absolute offset in the WAL file that
  // corresponds to byte 0 of this slot's scratch region.  Set by the
  // switcher in SwitchActive() before publishing the slot, and stable
  // for the lifetime of the slot's active cycle.  SerializeRecord needs
  // this because log-record block padding depends on
  // `(abs_file_offset % kBlockSize)`.  See design §3.2 / §3.10.
  //
  // in_flight_count tracks writers between ClaimSlot and ReleaseSlot.
  // Writers fetch_add(1, relaxed) under wal_write_mutex_; releasers
  // fetch_sub(1, release) lockless; the switcher's WaitDrain spins on
  // a load(acquire) until the count returns to zero.
  struct BufferSlot {
    std::atomic<uint64_t> in_flight_count;
    uint64_t next_wal_offset;
    uint64_t file_offset_start;
  };

  // checksum handoff (perform_data_verification / buffered_data_with_checksum)
  // is rejected at DB open when nonblocking_wal_io is enabled, so those
  // parameters are omitted here.
  // `use_group_commit` is the DB-level flag.  When false (leaderless write
  // path enabled), the ctor pre-grows buf_ and flush_buf_ to
  // kSlotBytesCapacity and aliases them as the two slot scratch regions
  // (slots_[0] ↔ buf_, slots_[1] ↔ flush_buf_).  In this mode the legacy
  // SwapBuffer/FlushSwappedBuffer path is never exercised.  When true,
  // the legacy ping-pong pre-allocation is used and the slot machinery is
  // dormant.  Caller must pass this explicitly — see design §4.4 Case 4.
  NonBlockingIOWritableFileWriter(
      std::unique_ptr<FSWritableFile>&& file, const std::string& file_name,
      const FileOptions& options, bool use_group_commit,
      SystemClock* clock = nullptr,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr,
      Statistics* stats = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {})
      : WritableFileWriter(std::move(file), file_name, options, clock,
                           io_tracer, stats, listeners,
                           /*file_checksum_gen_factory=*/nullptr,
                           /*perform_data_verification=*/false,
                           /*buffered_data_with_checksum=*/false),
        use_group_commit_(use_group_commit) {
    if (use_group_commit_) {
      // Legacy ping-pong: small pre-alloc for flush_buf_; buf_ grows via
      // Append().  Slot machinery is dormant in this mode.
      flush_buf_.Alignment(writable_file()->GetRequiredBufferAlignment());
      flush_buf_.AllocateNewBuffer(
          std::min((size_t)65536, max_buffer_size_));
    } else {
      // Leaderless write path: alias buf_ ↔ slots_[0], flush_buf_ ↔
      // slots_[1], each pre-grown to kSlotBytesCapacity (hard cap — app
      // must call SyncWAL() regularly, §4.9).  Alignment matches the
      // underlying file so writes can go direct-to-OS without staging.
      const size_t align = std::max<size_t>(
          kSlotBytesAlignment, writable_file()->GetRequiredBufferAlignment());
      buf_.Alignment(align);
      buf_.AllocateNewBuffer(kSlotBytesCapacity);
      flush_buf_.Alignment(align);
      flush_buf_.AllocateNewBuffer(kSlotBytesCapacity);
    }

    slots_[0].in_flight_count.store(0, std::memory_order_relaxed);
    slots_[0].next_wal_offset = 0;
    slots_[0].file_offset_start = 0;
    slots_[1].in_flight_count.store(0, std::memory_order_relaxed);
    slots_[1].next_wal_offset = 0;
    slots_[1].file_offset_start = 0;
    active_buffer_ptr_.store(&slots_[0], std::memory_order_relaxed);
  }

  // Swap buf_ into flush_buf_ so that FlushSwappedBuffer() can write it to
  // disk without holding the mutex while Append()s continue into the fresh
  // buf_. Must be called under wal_write_mutex_.
  void SwapBuffer();

  // Write flush_buf_ to the OS and issue Flush(). Safe to call without any
  // mutex because it does not touch buf_. Must follow SwapBuffer().
  IOStatus FlushSwappedBuffer(
      Env::IOPriority op_rate_limiter_priority = Env::IO_TOTAL);

  // Returns true when flush_buf_ is empty, i.e. no SwapBuffer() is pending
  // flush.  Useful for invariant checks by callers holding wal_write_mutex_.
  bool IsFlushBufEmpty() const { return flush_buf_.CurrentSize() == 0; }

  // ---- Slot primitives for the eliminate_group_commit write path ----

  // Returns the currently active slot.  Caller must hold wal_write_mutex_
  // — otherwise the active slot may change between this call and the
  // next operation.  Tests may call without the mutex when there is only
  // one thread.
  BufferSlot* ActiveSlot() {
    return active_buffer_ptr_.load(std::memory_order_relaxed);
  }
  // The "other" slot — useful for selecting B given A.
  BufferSlot* OtherSlot(BufferSlot* s) {
    return (s == &slots_[0]) ? &slots_[1] : &slots_[0];
  }
  BufferSlot* Slot(size_t i) { return &slots_[i]; }

  // ---- Per-slot byte scratch regions (eliminate_group_commit path) ----
  //
  // Each slot serializes WAL records into a contiguous scratch region at
  // the writer's reserved offset.  Per §4.4 Case 4: slots_[0] aliases
  // buf_, slots_[1] aliases flush_buf_ — the same AlignedBuffers the
  // legacy path uses, just never swapped in this mode.  Capacity is a
  // hard cap; the app contract is to call SyncWAL() regularly (§4.9).
  static constexpr size_t kSlotBytesAlignment = 4096;
  static constexpr size_t kSlotBytesCapacity = size_t{64} * 1024 * 1024;

  char* SlotBytesStart(BufferSlot* slot) {
    assert(!use_group_commit_);
    assert(slot == &slots_[0] || slot == &slots_[1]);
    return (slot == &slots_[0]) ? buf_.BufferStart() : flush_buf_.BufferStart();
  }
  size_t SlotBytesCapacity() const { return kSlotBytesCapacity; }

  // Writer claim protocol.  Caller must hold wal_write_mutex_.  Returns
  // the current active slot with in_flight_count already bumped; caller
  // must later call ReleaseSlot() on the same pointer.
  BufferSlot* ClaimSlot();

  // Writer release protocol.  Lockless decrement; pairs with ClaimSlot.
  void ReleaseSlot(BufferSlot* slot);

  // AdvanceFileSize(size_t n) is inherited from WritableFileWriter
  // (protected → exposed to the leaderless write path via `using` below).

  // Flush bytes [0 .. slot->next_wal_offset) of `slot` to the OS.  The
  // caller must have called SwitchActive(slot, ...) followed by
  // WaitDrain(slot), so `slot->next_wal_offset` is frozen.  Does not
  // update filesize_; flushed_size_ is updated inside WriteToFile().
  // Safe to call without wal_write_mutex_.
  IOStatus FlushSlotBytes(
      BufferSlot* slot,
      Env::IOPriority op_rate_limiter_priority = Env::IO_TOTAL);

  // Switch the active slot from A to B.  Caller must hold
  // wal_write_mutex_ and must pass the current active as A.  Freezes A's
  // offsets and publishes B as the new active slot; new writers' next
  // ClaimSlot will land in B.  The caller then drops the mutex and calls
  // WaitDrain(A) before flushing A.
  void SwitchActive(BufferSlot* A, BufferSlot* B);

  // Spin until A's in_flight_count returns to zero.  Caller must NOT
  // hold wal_write_mutex_ (writers need to be able to acquire it to
  // ReleaseSlot — though releases themselves are lockless, the mutex is
  // serializing other claims that might pile up).  Must be called only
  // after SwitchActive(A, ...) has published B as the new active.
  void WaitDrain(BufferSlot* slot);

  // Reset a fully-drained slot for reuse as the standby.  Resets
  // next_wal_offset; in_flight_count is already zero by WaitDrain.
  // Caller must own the slot post-flush.
  void ResetDrainedSlot(BufferSlot* slot);

  // Expose the base-class protected helper so the leaderless write path
  // (WriteImplNoGroup) can advance filesize_ without going through Append.
  using WritableFileWriter::AdvanceFileSize;

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

  // Two persistent slots for the eliminate_group_commit path.  In that
  // mode they alias buf_ (slot 0) and flush_buf_ (slot 1) for serialize
  // scratch.  On construction slot 0 is active.
  BufferSlot slots_[2];
  std::atomic<BufferSlot*> active_buffer_ptr_;

  // DB-level flag captured at construction.  When true (default), legacy
  // SwapBuffer/FlushSwappedBuffer path is used; when false, the
  // leaderless write path uses buf_/flush_buf_ as slot scratch regions.
  const bool use_group_commit_;
};

}  // namespace ROCKSDB_NAMESPACE
