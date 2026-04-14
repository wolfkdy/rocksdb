//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/lsn_offset_allocator.h"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "port/stack_trace.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

using Pair = LsnOffsetAllocator::Pair;

class LsnOffsetAllocatorTest : public testing::Test {};

TEST_F(LsnOffsetAllocatorTest, SingleThreadMonotonic) {
  LsnOffsetAllocator a(1000);
  uint64_t next_wal_offset = 500;

  Pair p1 = a.Advance(3, 50, &next_wal_offset);
  ASSERT_EQ(p1.start_lsn, 1000U);
  ASSERT_EQ(p1.start_offset, 500U);

  Pair p2 = a.Advance(1, 20, &next_wal_offset);
  ASSERT_EQ(p2.start_lsn, 1003U);
  ASSERT_EQ(p2.start_offset, 550U);

  Pair p3 = a.Advance(7, 100, &next_wal_offset);
  ASSERT_EQ(p3.start_lsn, 1004U);
  ASSERT_EQ(p3.start_offset, 570U);

  ASSERT_EQ(next_wal_offset, 670U);
  Pair peek = a.PeekPair(&next_wal_offset);
  ASSERT_EQ(peek.start_lsn, 1011U);
  ASSERT_EQ(peek.start_offset, 670U);
}

TEST_F(LsnOffsetAllocatorTest, ZeroAdvanceIsIdempotent) {
  LsnOffsetAllocator a(42);
  uint64_t off = 7;
  Pair p1 = a.Advance(0, 0, &off);
  ASSERT_EQ(p1.start_lsn, 42U);
  ASSERT_EQ(p1.start_offset, 7U);
  Pair p2 = a.Advance(0, 0, &off);
  ASSERT_EQ(p2.start_lsn, 42U);
  ASSERT_EQ(p2.start_offset, 7U);
}

TEST_F(LsnOffsetAllocatorTest, ResetClearsState) {
  LsnOffsetAllocator a(100);
  uint64_t off = 10;
  a.Advance(5, 30, &off);
  a.Reset(0);
  Pair p = a.Advance(1, 1, &off);
  ASSERT_EQ(p.start_lsn, 0U);
  ASSERT_EQ(p.start_offset, 40U);  // offset is caller-owned, not reset
}

// Partial-order invariant (§3.2.3): for any two writers in the same buffer,
// lsn ordering matches offset ordering.  Equivalently, sorting pairs by
// start_lsn must also sort them by start_offset.
TEST_F(LsnOffsetAllocatorTest, PartialOrderInvariantUnderContention) {
  constexpr int kThreads = 8;
  constexpr int kIters = 5000;
  LsnOffsetAllocator a(0);
  uint64_t next_wal_offset = 0;

  std::mutex pairs_mu;
  std::vector<Pair> pairs;
  pairs.reserve(kThreads * kIters);

  std::atomic<int> ready{0};
  std::vector<std::thread> ts;
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&, t]() {
      // Deterministic, varying sizes so the invariant is non-trivial.
      uint64_t lsn_count = 1 + (t % 3);
      uint64_t byte_count = 8 + (t % 5) * 4;
      std::vector<Pair> local;
      local.reserve(kIters);
      ready.fetch_add(1);
      while (ready.load() < kThreads) {
      }
      for (int i = 0; i < kIters; ++i) {
        local.push_back(a.Advance(lsn_count, byte_count, &next_wal_offset));
      }
      {
        std::lock_guard<std::mutex> g(pairs_mu);
        for (auto& p : local) pairs.push_back(p);
      }
    });
  }
  for (auto& th : ts) th.join();

  std::sort(pairs.begin(), pairs.end(),
            [](const Pair& x, const Pair& y) {
              return x.start_lsn < y.start_lsn;
            });
  // After sorting by start_lsn, start_offset must also be strictly
  // non-decreasing: two writers never share a start_lsn (advance is atomic
  // with nonzero lsn_count).
  for (size_t i = 1; i < pairs.size(); ++i) {
    ASSERT_LT(pairs[i - 1].start_lsn, pairs[i].start_lsn)
        << "duplicate start_lsn at index " << i;
    ASSERT_LT(pairs[i - 1].start_offset, pairs[i].start_offset)
        << "partial-order invariant violated at index " << i;
  }
}

// Mid-advance reads via PeekPair must never observe an inconsistent pair.
// The invariant we check: after a reader's PeekPair returns (L, O), the
// number of bytes already advanced into next_wal_offset is exactly the sum
// of byte_counts from all writers whose lsn_count has landed in next_lsn_
// below L.  With fixed-size writers (lsn_count=1, byte_count=K), this
// reduces to `O == L * K` at all times.
TEST_F(LsnOffsetAllocatorTest, NoTornReadsUnderContention) {
  constexpr int kWriters = 6;
  constexpr int kIters = 10000;
  constexpr uint64_t kBytesPerLsn = 17;
  LsnOffsetAllocator a(0);
  uint64_t next_wal_offset = 0;

  std::atomic<bool> stop{false};
  std::atomic<int> ready{0};
  std::vector<std::thread> writers;
  for (int t = 0; t < kWriters; ++t) {
    writers.emplace_back([&]() {
      ready.fetch_add(1);
      while (ready.load() < kWriters + 1) {
      }
      for (int i = 0; i < kIters; ++i) {
        a.Advance(1, kBytesPerLsn, &next_wal_offset);
      }
    });
  }
  std::atomic<uint64_t> peeks{0};
  std::thread reader([&]() {
    ready.fetch_add(1);
    while (ready.load() < kWriters + 1) {
    }
    while (!stop.load(std::memory_order_acquire)) {
      Pair p = a.PeekPair(&next_wal_offset);
      ASSERT_EQ(p.start_offset, p.start_lsn * kBytesPerLsn)
          << "torn read: lsn=" << p.start_lsn
          << " offset=" << p.start_offset;
      peeks.fetch_add(1);
    }
  });
  for (auto& w : writers) w.join();
  // One more check after writers finish.
  Pair final_p = a.PeekPair(&next_wal_offset);
  ASSERT_EQ(final_p.start_lsn, uint64_t{kWriters} * kIters);
  ASSERT_EQ(final_p.start_offset, final_p.start_lsn * kBytesPerLsn);
  stop.store(true, std::memory_order_release);
  reader.join();
  ASSERT_GT(peeks.load(), 0U);
}

// Two buffers: writers rotate between two offset slots.  Within each buffer,
// the partial-order invariant must hold independently.
TEST_F(LsnOffsetAllocatorTest, PartialOrderAcrossBuffers) {
  constexpr int kThreads = 4;
  constexpr int kIters = 2000;
  LsnOffsetAllocator a(0);
  uint64_t off_a = 0;
  uint64_t off_b = 0;

  struct LocalPair { Pair p; int buf; };
  std::mutex pairs_mu;
  std::vector<LocalPair> pairs;

  std::atomic<int> ready{0};
  std::vector<std::thread> ts;
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&, t]() {
      std::vector<LocalPair> local;
      local.reserve(kIters);
      ready.fetch_add(1);
      while (ready.load() < kThreads) {
      }
      for (int i = 0; i < kIters; ++i) {
        // Thread t sends all its writes to buffer (t % 2).  This mimics
        // the per-buffer writer binding in the real write path — each
        // writer's buffer is fixed for the duration of a single Advance.
        int buf = t % 2;
        uint64_t* off = (buf == 0) ? &off_a : &off_b;
        local.push_back({a.Advance(1, 10, off), buf});
      }
      {
        std::lock_guard<std::mutex> g(pairs_mu);
        for (auto& p : local) pairs.push_back(p);
      }
    });
  }
  for (auto& th : ts) th.join();

  // Separate pairs per buffer.
  std::vector<Pair> buf_pairs[2];
  for (auto& lp : pairs) buf_pairs[lp.buf].push_back(lp.p);
  for (int b = 0; b < 2; ++b) {
    auto& v = buf_pairs[b];
    std::sort(v.begin(), v.end(), [](const Pair& x, const Pair& y) {
      return x.start_lsn < y.start_lsn;
    });
    for (size_t i = 1; i < v.size(); ++i) {
      ASSERT_LT(v[i - 1].start_lsn, v[i].start_lsn);
      ASSERT_LT(v[i - 1].start_offset, v[i].start_offset)
          << "partial-order violated in buffer " << b;
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
