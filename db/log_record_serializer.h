//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
namespace log {

// Bytes-only equivalent of Writer::AddRecord.  Serializes a record to a
// caller-provided buffer instead of appending it through a WritableFileWriter,
// producing byte-for-byte identical output to what Writer::AddRecord would
// emit at the same absolute file offset.
//
// Used by the eliminate_group_commit write path (see
// eliminate_group_commit_design.md §3.10 and §1.3 in the impl plan).  Each
// writer serializes its own record in parallel, then memcpy's the result
// into the active WAL buffer at its reserved offset.
//
// Constraints:
//   - Compression is NOT supported.  The eliminate_group_commit path
//     disallows WAL compression; callers must reject it upfront.
//   - payload.size() <= 0xFFFF on a single record is NOT required at this
//     layer — the serializer fragments larger payloads across 32KB blocks
//     using FirstType / MiddleType / LastType fragments, exactly as
//     Writer::AddRecord does.

// Returns the total number of bytes `SerializeRecord(payload, abs_file_offset,
// ...)` will write, including any mid-block zero padding caused by block
// boundary crossings.  Depends on `abs_file_offset` because padding is driven
// by the current position in the 32KB block.
size_t ComputeSerializedRecordSize(size_t payload_size,
                                   uint64_t abs_file_offset,
                                   bool recycle_log_files);

// Serializes `payload` into `dst` exactly as Writer::AddRecord would emit it
// when called with the writer's internal `block_offset_ == abs_file_offset %
// kBlockSize`.  Returns the number of bytes written to `dst`.
//
// `dst` must have at least
// ComputeSerializedRecordSize(payload.size(), abs_file_offset, recycle) bytes.
size_t SerializeRecord(const Slice& payload, uint64_t abs_file_offset,
                       uint64_t log_number, bool recycle_log_files, char* dst);

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
