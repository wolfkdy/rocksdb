//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/log_record_serializer.h"

#include <cassert>
#include <cstring>

#include "db/log_format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
namespace log {

namespace {

// Byte-level writer that emits one physical record into a caller-provided
// buffer.  Keeps state identical to log::Writer's block_offset_ so that
// fragmentation matches.
struct SerializerState {
  int header_size;
  uint64_t log_number;
  char* dst;
  size_t out_bytes;
  size_t block_offset;  // current offset within the 32KB block
};

// Matches Writer::EmitPhysicalRecord bit-for-bit, but writes to dst instead of
// the underlying file.
void EmitPhysicalRecord(SerializerState* s, RecordType type, const char* ptr,
                        size_t n) {
  assert(n <= 0xFFFF);
  const size_t hdr_size =
      (type < kRecyclableFullType || type == kSetCompressionType)
          ? static_cast<size_t>(kHeaderSize)
          : static_cast<size_t>(kRecyclableHeaderSize);
  (void)hdr_size;
  assert(static_cast<size_t>(s->header_size) == hdr_size);
  assert(s->block_offset + s->header_size + n <= kBlockSize);

  char* buf = s->dst + s->out_bytes;
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(type);

  // CRC layout mirrors Writer::EmitPhysicalRecord:
  //   legacy:     crc(type_byte) extended with crc(payload)
  //   recyclable: crc(type_byte) extended with crc(log_num_bytes) extended
  //               with crc(payload)
  char type_byte = static_cast<char>(type);
  uint32_t crc = crc32c::Value(&type_byte, 1);
  if (type >= kRecyclableFullType && type != kSetCompressionType) {
    EncodeFixed32(buf + 7, static_cast<uint32_t>(s->log_number));
    crc = crc32c::Extend(crc, buf + 7, 4);
  }
  uint32_t payload_crc = crc32c::Value(ptr, n);
  crc = crc32c::Crc32cCombine(crc, payload_crc, n);
  crc = crc32c::Mask(crc);
  EncodeFixed32(buf, crc);

  std::memcpy(buf + s->header_size, ptr, n);
  s->out_bytes += s->header_size + n;
  s->block_offset += s->header_size + n;
}

// Shared block-boundary bookkeeping.  Returns bytes consumed by padding (0
// when no pad was written).  Zeroes out the padding region in `dst` if
// `dst != nullptr` (so it matches Writer::AddRecord's explicit zero-pad).
size_t MaybePadForNewBlock(size_t* block_offset, int header_size,
                           char* dst_pad_region) {
  const int64_t leftover =
      static_cast<int64_t>(kBlockSize) - static_cast<int64_t>(*block_offset);
  assert(leftover >= 0);
  if (leftover < header_size) {
    if (leftover > 0) {
      if (dst_pad_region != nullptr) {
        std::memset(dst_pad_region, 0, static_cast<size_t>(leftover));
      }
    }
    *block_offset = 0;
    return static_cast<size_t>(leftover);
  }
  return 0;
}

}  // namespace

size_t ComputeSerializedRecordSize(size_t payload_size,
                                   uint64_t abs_file_offset,
                                   bool recycle_log_files) {
  const int header_size = recycle_log_files ? kRecyclableHeaderSize : kHeaderSize;
  size_t block_offset = static_cast<size_t>(abs_file_offset % kBlockSize);
  size_t left = payload_size;
  size_t out = 0;

  // Mirror the do/while in Writer::AddRecord so a zero-length payload still
  // emits exactly one zero-payload record.
  do {
    out += MaybePadForNewBlock(&block_offset, header_size, nullptr);
    assert(static_cast<int64_t>(kBlockSize - block_offset) >= header_size);
    const size_t avail = kBlockSize - block_offset - header_size;
    const size_t fragment_length = (left < avail) ? left : avail;
    out += header_size + fragment_length;
    block_offset += header_size + fragment_length;
    left -= fragment_length;
  } while (left > 0);

  return out;
}

size_t SerializeRecord(const Slice& payload, uint64_t abs_file_offset,
                       uint64_t log_number, bool recycle_log_files,
                       char* dst) {
  SerializerState s;
  s.header_size = recycle_log_files ? kRecyclableHeaderSize : kHeaderSize;
  s.log_number = log_number;
  s.dst = dst;
  s.out_bytes = 0;
  s.block_offset = static_cast<size_t>(abs_file_offset % kBlockSize);

  const char* ptr = payload.data();
  size_t left = payload.size();
  bool begin = true;
  do {
    s.out_bytes += MaybePadForNewBlock(&s.block_offset, s.header_size,
                                       s.dst + s.out_bytes);
    assert(static_cast<int64_t>(kBlockSize - s.block_offset) >= s.header_size);
    const size_t avail = kBlockSize - s.block_offset - s.header_size;
    const size_t fragment_length = (left < avail) ? left : avail;
    const bool end = (left == fragment_length);
    RecordType type;
    if (begin && end) {
      type = recycle_log_files ? kRecyclableFullType : kFullType;
    } else if (begin) {
      type = recycle_log_files ? kRecyclableFirstType : kFirstType;
    } else if (end) {
      type = recycle_log_files ? kRecyclableLastType : kLastType;
    } else {
      type = recycle_log_files ? kRecyclableMiddleType : kMiddleType;
    }
    EmitPhysicalRecord(&s, type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (left > 0);

  return s.out_bytes;
}

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
