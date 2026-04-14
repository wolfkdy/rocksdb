//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/preprocess_write_lockless.h"

#include "rocksdb/write_batch.h"
#include "test_util/testharness.h"
#include "util/link_buf.h"

namespace ROCKSDB_NAMESPACE {

class EvaluatePrewriteConditionsTest : public testing::Test {};

TEST_F(EvaluatePrewriteConditionsTest, AllFalseReturnsOk) {
  PrewriteConditions c;
  ASSERT_EQ(PrewriteReason::kOk, EvaluatePrewriteConditions(c));
}

TEST_F(EvaluatePrewriteConditionsTest, DbStoppedHasHighestPriority) {
  PrewriteConditions c;
  c.db_stopped = true;
  c.wal_too_big = true;
  c.wbm_should_flush = true;
  c.trim_history_pending = true;
  c.flush_pending = true;
  c.need_log_sync = true;
  c.write_stalled = true;
  c.wbm_should_stall = true;
  ASSERT_EQ(PrewriteReason::kDbStopped, EvaluatePrewriteConditions(c));
}

TEST_F(EvaluatePrewriteConditionsTest, WalTooBigBeatsRest) {
  PrewriteConditions c;
  c.wal_too_big = true;
  c.wbm_should_flush = true;
  c.flush_pending = true;
  c.write_stalled = true;
  c.need_log_sync = true;
  ASSERT_EQ(PrewriteReason::kWalTooBig, EvaluatePrewriteConditions(c));
}

TEST_F(EvaluatePrewriteConditionsTest, WbmShouldFlushBeatsTrimAndFlush) {
  PrewriteConditions c;
  c.wbm_should_flush = true;
  c.trim_history_pending = true;
  c.flush_pending = true;
  ASSERT_EQ(PrewriteReason::kWbmShouldFlush, EvaluatePrewriteConditions(c));
}

TEST_F(EvaluatePrewriteConditionsTest, TrimBeatsFlush) {
  PrewriteConditions c;
  c.trim_history_pending = true;
  c.flush_pending = true;
  ASSERT_EQ(PrewriteReason::kTrimHistoryPending,
            EvaluatePrewriteConditions(c));
}

TEST_F(EvaluatePrewriteConditionsTest, FlushPendingFiresAlone) {
  PrewriteConditions c;
  c.flush_pending = true;
  ASSERT_EQ(PrewriteReason::kFlushPending, EvaluatePrewriteConditions(c));
}

TEST_F(EvaluatePrewriteConditionsTest, WriteStalledBeatsWbmStall) {
  PrewriteConditions c;
  c.write_stalled = true;
  c.wbm_should_stall = true;
  c.need_log_sync = true;
  ASSERT_EQ(PrewriteReason::kWriteStalled, EvaluatePrewriteConditions(c));
}

TEST_F(EvaluatePrewriteConditionsTest, WbmStallBeatsLogSync) {
  PrewriteConditions c;
  c.wbm_should_stall = true;
  c.need_log_sync = true;
  ASSERT_EQ(PrewriteReason::kWbmShouldStall, EvaluatePrewriteConditions(c));
}

TEST_F(EvaluatePrewriteConditionsTest, NeedLogSyncIsLowest) {
  PrewriteConditions c;
  c.need_log_sync = true;
  ASSERT_EQ(PrewriteReason::kNeedLogSync, EvaluatePrewriteConditions(c));
}

class ValidateWriteBatchTest : public testing::Test {};

TEST_F(ValidateWriteBatchTest, NullBatchIsOk) {
  PrewriteResult r = ValidateWriteBatch(nullptr, 0);
  ASSERT_EQ(PrewriteReason::kOk, r.reason);
}

TEST_F(ValidateWriteBatchTest, EmptyBatchOk) {
  WriteBatch b;
  PrewriteResult r = ValidateWriteBatch(&b, 0);
  ASSERT_EQ(PrewriteReason::kOk, r.reason);
}

TEST_F(ValidateWriteBatchTest, SmallBatchOk) {
  WriteBatch b(0, 0, 8, 0);
  ASSERT_OK(b.Put("k", "v"));
  PrewriteResult r = ValidateWriteBatch(&b, 1);
  ASSERT_EQ(PrewriteReason::kOk, r.reason);
}

TEST_F(ValidateWriteBatchTest, TooLargeBatchRejected) {
  WriteBatch b;
  PrewriteResult r = ValidateWriteBatch(&b, LinkBuf::kCapacity);
  ASSERT_EQ(PrewriteReason::kBatchTooLarge, r.reason);
  ASSERT_TRUE(r.validation_status.IsBusy());
}

TEST_F(ValidateWriteBatchTest, AtCapacityMinusOneOk) {
  WriteBatch b;
  PrewriteResult r = ValidateWriteBatch(&b, LinkBuf::kCapacity - 1);
  ASSERT_EQ(PrewriteReason::kOk, r.reason);
}

TEST_F(ValidateWriteBatchTest, CorruptBatchDetected) {
  WriteBatch b(0, 0, 8, 0);
  ASSERT_OK(b.Put("key1", "val1"));
  ASSERT_OK(b.Put("key2", "val2"));
  // Mutate the serialized rep after WriteBatch builds per-key protection
  // info — the checksum should mismatch.
  std::string* rep = const_cast<std::string*>(&b.Data());
  ASSERT_FALSE(rep->empty());
  // Flip a byte in the middle of the payload.  Skip the 12-byte header.
  ASSERT_GT(rep->size(), 20u);
  (*rep)[rep->size() - 2] ^= 0xFF;
  PrewriteResult r = ValidateWriteBatch(&b, 2);
  ASSERT_EQ(PrewriteReason::kBatchCorrupt, r.reason);
  ASSERT_FALSE(r.validation_status.ok());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
