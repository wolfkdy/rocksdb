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
  assert(flush_buf_.CurrentSize() == 0);
  // Ping-pong: move buf_ -> flush_buf_ for writing; reuse flush_buf_'s old
  // allocation (now empty) as the new buf_ for incoming Append()s.
  std::swap(buf_, flush_buf_);
  // Ensure buf_ has an allocation (first swap, or after flush_buf_ was
  // freshly constructed).
  if (buf_.Capacity() == 0) {
    buf_.Alignment(writable_file()->GetRequiredBufferAlignment());
    buf_.AllocateNewBuffer(std::min((size_t)65536, max_buffer_size_));
  }
}

IOStatus NonBlockingIOWritableFileWriter::FlushSwappedBuffer(
    Env::IOPriority op_rate_limiter_priority) {
  if (seen_error()) {
    return AssertFalseAndGetStatusForPrevError();
  }
  // Double-buffering is only for buffered I/O; direct I/O has its own path.
  assert(!use_direct_io());
  IOStatus s;

  if (flush_buf_.CurrentSize() > 0) {
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

  {
    IOOptions io_options;
    io_options.rate_limiter_priority = DecideRateLimiterPriority(
        writable_file()->GetIOPriority(), op_rate_limiter_priority);
    s = writable_file()->Flush(io_options, nullptr);
  }

  if (!s.ok()) {
    set_seen_error();
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
