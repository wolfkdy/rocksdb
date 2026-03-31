// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once
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
class NonBlockingIOWritableFileWriter : public WritableFileWriter {
 public:
  NonBlockingIOWritableFileWriter(
      std::unique_ptr<FSWritableFile>&& file,
      const std::string& file_name,
      const FileOptions& options,
      SystemClock* clock = nullptr,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr,
      Statistics* stats = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {},
      FileChecksumGenFactory* file_checksum_gen_factory = nullptr,
      bool perform_data_verification = false,
      bool buffered_data_with_checksum = false)
      : WritableFileWriter(std::move(file), file_name, options, clock,
                           io_tracer, stats, listeners,
                           file_checksum_gen_factory,
                           perform_data_verification,
                           buffered_data_with_checksum) {}

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
};

}  // namespace ROCKSDB_NAMESPACE
