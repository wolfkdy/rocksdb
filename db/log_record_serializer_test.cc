//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/log_record_serializer.h"

#include <random>
#include <string>
#include <vector>

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "port/stack_trace.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {
namespace log {

namespace {

// Minimal FSSequentialFile over an in-memory string — just enough to drive
// log::Reader.  log_test.cc has a fancier version; we keep this local so the
// test has no hidden dependency on that file's helpers.
class StringSource : public FSSequentialFile {
 public:
  explicit StringSource(Slice contents) : contents_(contents) {}
  IOStatus Read(size_t n, const IOOptions&, Slice* result, char* scratch,
                IODebugContext*) override {
    if (contents_.size() < n) {
      n = contents_.size();
    }
    std::memcpy(scratch, contents_.data(), n);
    *result = Slice(scratch, n);
    contents_.remove_prefix(n);
    return IOStatus::OK();
  }
  IOStatus Skip(uint64_t n) override {
    if (n > contents_.size()) {
      contents_ = Slice();
      return IOStatus::NotFound("skipped past end");
    }
    contents_.remove_prefix(n);
    return IOStatus::OK();
  }

 private:
  Slice contents_;
};

class SilentReporter : public Reader::Reporter {
 public:
  void Corruption(size_t bytes, const Status& s) override {
    corruption_bytes_ += bytes;
    last_status_ = s;
  }
  size_t corruption_bytes_ = 0;
  Status last_status_;
};

std::string MakePayload(size_t size, uint32_t seed) {
  std::string s;
  s.resize(size);
  std::mt19937 rng(seed);
  for (size_t i = 0; i < size; ++i) {
    s[i] = static_cast<char>(rng() & 0xff);
  }
  return s;
}

}  // namespace

class LogRecordSerializerTest : public testing::TestWithParam<bool> {
 public:
  bool recycle() const { return GetParam(); }
};

// Serializer output must be byte-for-byte identical to Writer::AddRecord for
// the same payload at the same absolute file offset.
TEST_P(LogRecordSerializerTest, MatchesWriterBytewise) {
  const uint64_t kLogNumber = 42;

  std::vector<size_t> sizes = {0, 1, 100, 31 * 1024, 32 * 1024,
                               33 * 1024, 100 * 1024, 1024 * 1024};

  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> sink_holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(sink_holder), "" /* file name */, FileOptions()));
  Writer writer(std::move(file_writer), kLogNumber, recycle(),
                /*manual_flush=*/false);
  const std::string& sink_contents = sink->contents_;

  uint64_t offset = 0;
  uint32_t seed = 1;
  for (size_t sz : sizes) {
    std::string payload = MakePayload(sz, seed++);
    size_t before = sink_contents.size();
    ASSERT_OK(writer.AddRecord(Slice(payload)));
    size_t after = sink_contents.size();

    size_t expected_size =
        ComputeSerializedRecordSize(sz, offset, recycle());
    ASSERT_EQ(expected_size, after - before)
        << "size mismatch for payload " << sz << " at offset " << offset;

    std::string buf(expected_size, '\0');
    size_t n = SerializeRecord(Slice(payload), offset, kLogNumber, recycle(),
                               buf.data());
    ASSERT_EQ(n, expected_size);
    ASSERT_EQ(buf, sink_contents.substr(before, expected_size))
        << "bytes differ for payload " << sz << " at offset " << offset;

    offset = after;
  }
}

// Round-trip: serialize via SerializeRecord, then read via log::Reader.
TEST_P(LogRecordSerializerTest, RoundTripsThroughReader) {
  const uint64_t kLogNumber = 42;
  std::vector<size_t> sizes = {0,          1,          100,       31 * 1024,
                               32 * 1024,  33 * 1024,  100 * 1024};

  std::string contents;
  uint64_t offset = 0;
  uint32_t seed = 7;
  std::vector<std::string> payloads;
  for (size_t sz : sizes) {
    payloads.push_back(MakePayload(sz, seed++));
    size_t rec_size =
        ComputeSerializedRecordSize(sz, offset, recycle());
    size_t old_size = contents.size();
    contents.resize(old_size + rec_size);
    size_t n =
        SerializeRecord(Slice(payloads.back()), offset, kLogNumber, recycle(),
                        &contents[old_size]);
    ASSERT_EQ(n, rec_size);
    offset += rec_size;
  }

  std::unique_ptr<FSSequentialFile> source(new StringSource(Slice(contents)));
  std::unique_ptr<SequentialFileReader> file_reader(
      new SequentialFileReader(std::move(source), "" /* file name */));
  SilentReporter reporter;
  Reader reader(/*info_log=*/nullptr, std::move(file_reader), &reporter,
                /*checksum=*/true, kLogNumber);

  for (size_t i = 0; i < payloads.size(); ++i) {
    Slice record;
    std::string scratch;
    ASSERT_TRUE(reader.ReadRecord(&record, &scratch));
    ASSERT_EQ(record.ToString(), payloads[i]);
  }
  ASSERT_EQ(reporter.corruption_bytes_, 0U);
}

// Torn tail: truncate output at various points and verify Reader stops
// cleanly rather than treating the tail as a valid record.
TEST_P(LogRecordSerializerTest, TornTailStopsCleanly) {
  const uint64_t kLogNumber = 42;
  // Use a batch that is comfortably larger than one block so we have a
  // multi-physical-record entry; truncating mid-fragment exercises the
  // reader's corrupted-tail handling.
  std::string payload = MakePayload(200 * 1024, 99);
  size_t rec_size = ComputeSerializedRecordSize(payload.size(), 0, recycle());
  std::string contents(rec_size, '\0');
  SerializeRecord(Slice(payload), 0, kLogNumber, recycle(), contents.data());

  for (size_t trunc : {size_t{1}, size_t{10}, size_t{kBlockSize},
                       size_t{kBlockSize * 3},
                       rec_size - 1}) {
    std::string truncated = contents.substr(0, trunc);
    std::unique_ptr<FSSequentialFile> source(
        new StringSource(Slice(truncated)));
    std::unique_ptr<SequentialFileReader> file_reader(
        new SequentialFileReader(std::move(source), "" /* file name */));
    SilentReporter reporter;
    Reader reader(nullptr, std::move(file_reader), &reporter,
                  /*checksum=*/true, kLogNumber);
    Slice record;
    std::string scratch;
    // Reader should not return the truncated record as successfully read.
    // It may return false immediately, or report corruption.  Both outcomes
    // are acceptable — the forbidden outcome is returning the payload bytes
    // as if they were a complete record.
    bool got = reader.ReadRecord(&record, &scratch);
    if (got) {
      ASSERT_NE(record.ToString(), payload);
    }
  }
}

INSTANTIATE_TEST_CASE_P(LegacyAndRecyclable, LogRecordSerializerTest,
                        testing::Bool());

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
