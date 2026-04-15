// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "util/lsn_offset_allocator.h"

#include "db/log_record_serializer.h"

namespace ROCKSDB_NAMESPACE {

LsnOffsetAllocator::Pair LsnOffsetAllocator::Advance(uint64_t lsn_count,
                                                    uint64_t byte_count,
                                                    uint64_t* next_wal_offset) {
  Pair out;
  mu_.lock();
  out.start_lsn = next_lsn_;
  out.start_offset = *next_wal_offset;
  next_lsn_ += lsn_count;
  *next_wal_offset += byte_count;
  mu_.unlock();
  return out;
}

LsnOffsetAllocator::AdvanceResult LsnOffsetAllocator::AdvanceForSerialize(
    uint64_t lsn_count, size_t payload_size, uint64_t slot_file_offset_start,
    bool recycle_log_files, uint64_t* next_wal_offset) {
  AdvanceResult out;
  mu_.lock();
  out.start_lsn = next_lsn_;
  out.start_offset = *next_wal_offset;
  const uint64_t abs_file_offset = slot_file_offset_start + *next_wal_offset;
  out.byte_count = static_cast<uint64_t>(log::ComputeSerializedRecordSize(
      payload_size, abs_file_offset, recycle_log_files));
  next_lsn_ += lsn_count;
  *next_wal_offset += out.byte_count;
  mu_.unlock();
  return out;
}

LsnOffsetAllocator::Pair LsnOffsetAllocator::PeekPair(
    uint64_t* next_wal_offset) const {
  Pair out;
  mu_.lock();
  out.start_lsn = next_lsn_;
  out.start_offset = *next_wal_offset;
  mu_.unlock();
  return out;
}

}  // namespace ROCKSDB_NAMESPACE
