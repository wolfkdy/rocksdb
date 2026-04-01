// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "file/nonblocking_io_writable_file_writer.h"

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

}  // namespace ROCKSDB_NAMESPACE
