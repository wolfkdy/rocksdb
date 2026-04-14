//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/link_buf.h"

#include <atomic>
#include <thread>
#include <vector>

#include "port/port.h"
#include "port/stack_trace.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class LinkBufTest : public testing::Test {};

TEST_F(LinkBufTest, InitialWatermark) {
  LinkBuf lb;
  lb.Reset(100);
  ASSERT_EQ(100U, lb.MinNonHoleLsn());
}

TEST_F(LinkBufTest, SingleBatchAdvances) {
  LinkBuf lb;
  lb.Reset(10);
  lb.Store(10, 15);
  ASSERT_EQ(15U, lb.AdvanceWatermark());
  ASSERT_EQ(15U, lb.MinNonHoleLsn());
}

TEST_F(LinkBufTest, ChainedBatchesAdvanceInOneCall) {
  LinkBuf lb;
  lb.Reset(10);
  lb.Store(10, 15);
  lb.Store(15, 20);
  lb.Store(20, 42);
  // One call should walk all three hops.
  ASSERT_EQ(42U, lb.AdvanceWatermark());
}

TEST_F(LinkBufTest, HoleBlocksAdvance) {
  LinkBuf lb;
  lb.Reset(10);
  // Gap: no batch stores at start=10.
  lb.Store(15, 20);
  ASSERT_EQ(10U, lb.AdvanceWatermark());
  // Fill the hole — now both batches are visible.
  lb.Store(10, 15);
  ASSERT_EQ(20U, lb.AdvanceWatermark());
}

TEST_F(LinkBufTest, AdvanceIsIdempotent) {
  LinkBuf lb;
  lb.Reset(10);
  lb.Store(10, 15);
  ASSERT_EQ(15U, lb.AdvanceWatermark());
  // Running it again should be a no-op — the watermark does not go backwards
  // and there is no next batch to advance into.
  ASSERT_EQ(15U, lb.AdvanceWatermark());
}

TEST_F(LinkBufTest, SlotIsRecycledAfterAdvance) {
  LinkBuf lb;
  lb.Reset(0);
  // Many tiny batches; the ring wraps repeatedly.  This exercises the
  // recycle path: each slot is reused on wraparound.
  const uint64_t kTotal = LinkBuf::kCapacity * 4;
  for (uint64_t i = 0; i < kTotal; ++i) {
    lb.Store(i, i + 1);
    lb.AdvanceWatermark();
  }
  ASSERT_EQ(kTotal, lb.MinNonHoleLsn());
}

TEST_F(LinkBufTest, CheckBatchSizeRejectsOversize) {
  ASSERT_OK(LinkBuf::CheckBatchSize(1));
  ASSERT_OK(LinkBuf::CheckBatchSize(LinkBuf::kCapacity - 1));
  ASSERT_TRUE(LinkBuf::CheckBatchSize(LinkBuf::kCapacity).IsBusy());
  ASSERT_TRUE(LinkBuf::CheckBatchSize(LinkBuf::kCapacity + 1).IsBusy());
}

TEST_F(LinkBufTest, ConcurrentStoresAndAdvances) {
  // N writer threads each produce a sequential chain of 1-LSN batches.  A
  // single advance thread races alongside them.  After everyone finishes,
  // the watermark must reach the total count.
  constexpr int kThreads = 8;
  constexpr uint64_t kPerThread = 5000;
  LinkBuf lb;
  lb.Reset(0);

  // To stress concurrent stores without any inter-thread LSN coordination,
  // we partition LSNs deterministically: thread t handles LSNs
  // {t, t+kThreads, t+2*kThreads, ...}.  This creates interleaved stores
  // into the ring that must all resolve before the watermark can advance.
  std::atomic<int> ready{0};
  std::vector<std::thread> writers;
  for (int t = 0; t < kThreads; ++t) {
    writers.emplace_back([&, t]() {
      ready.fetch_add(1);
      while (ready.load() < kThreads) {
        /* spin */
      }
      for (uint64_t i = 0; i < kPerThread; ++i) {
        uint64_t lsn = t + i * kThreads;
        lb.Store(lsn, lsn + 1);
      }
    });
  }
  std::atomic<bool> stop{false};
  std::thread advancer([&]() {
    while (!stop.load()) {
      lb.AdvanceWatermark();
    }
  });
  for (auto& w : writers) {
    w.join();
  }
  // Drain any remaining holes.
  for (int i = 0; i < 10; ++i) {
    lb.AdvanceWatermark();
  }
  stop.store(true);
  advancer.join();

  ASSERT_EQ(static_cast<uint64_t>(kThreads) * kPerThread, lb.MinNonHoleLsn());
}

TEST_F(LinkBufTest, WatermarkIsMonotonic) {
  // Repeatedly advance concurrently with stores — no load ever exceeds the
  // latest stored end.
  constexpr uint64_t kBatches = 10000;
  LinkBuf lb;
  lb.Reset(0);
  std::atomic<uint64_t> produced{0};
  std::thread producer([&]() {
    for (uint64_t i = 0; i < kBatches; ++i) {
      lb.Store(i, i + 1);
      produced.fetch_add(1, std::memory_order_release);
    }
  });
  std::thread checker([&]() {
    uint64_t last = 0;
    while (produced.load(std::memory_order_acquire) < kBatches ||
           lb.MinNonHoleLsn() < kBatches) {
      uint64_t w = lb.AdvanceWatermark();
      ASSERT_GE(w, last);
      ASSERT_LE(w, kBatches);
      last = w;
    }
  });
  producer.join();
  checker.join();
  ASSERT_EQ(kBatches, lb.MinNonHoleLsn());
}

TEST_F(LinkBufTest, CheckRingCapacityPasses) {
  LinkBuf lb;
  lb.Reset(0);
  // Well inside capacity.
  lb.CheckRingCapacityOrAbort(0, 100);
  lb.CheckRingCapacityOrAbort(LinkBuf::kCapacity - 2, 1);
}

#ifdef GTEST_HAS_DEATH_TEST
TEST_F(LinkBufTest, RingFullAborts) {
  LinkBuf lb;
  lb.Reset(0);
  // start + count - watermark == kCapacity → abort.
  ASSERT_DEATH({ lb.CheckRingCapacityOrAbort(LinkBuf::kCapacity - 1, 1); }, "");
}
#endif

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
