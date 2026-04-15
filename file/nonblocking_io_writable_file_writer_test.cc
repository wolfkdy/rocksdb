//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "file/nonblocking_io_writable_file_writer.h"

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "port/stack_trace.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

using Slot = NonBlockingIOWritableFileWriter::BufferSlot;
using SwitchResult = NonBlockingIOWritableFileWriter::SwitchResult;

class NonBlockingSlotTest : public testing::Test {
 public:
  NonBlockingSlotTest() {
    std::unique_ptr<FSWritableFile> sink_holder(new test::StringSink());
    writer_.reset(new NonBlockingIOWritableFileWriter(
        std::move(sink_holder), "" /* file name */, FileOptions(),
        /*use_group_commit=*/false));
  }

  std::unique_ptr<NonBlockingIOWritableFileWriter> writer_;
};

TEST_F(NonBlockingSlotTest, InitialStateIsActiveAndFree) {
  Slot* a = writer_->ActiveSlot();
  Slot* b = writer_->OtherSlot(a);
  ASSERT_NE(a, b);
  uint64_t ah = a->header.load(std::memory_order_acquire);
  uint64_t bh = b->header.load(std::memory_order_acquire);
  ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderState(ah),
            uint64_t{NonBlockingIOWritableFileWriter::kBufferActive});
  ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderCount(ah), 0U);
  ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderState(bh),
            uint64_t{NonBlockingIOWritableFileWriter::kBufferFree});
  ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderCount(bh), 0U);
  ASSERT_EQ(a->next_wal_offset, 0U);
  ASSERT_EQ(b->next_wal_offset, 0U);
}

TEST_F(NonBlockingSlotTest, ClaimAndReleaseAdjustsCount) {
  Slot* s1 = writer_->ClaimSlot();
  Slot* s2 = writer_->ClaimSlot();
  ASSERT_EQ(s1, s2);  // both land in the current active slot
  ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderCount(
                s1->header.load(std::memory_order_acquire)),
            2U);
  writer_->ReleaseSlot(s1);
  writer_->ReleaseSlot(s2);
  ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderCount(
                s1->header.load(std::memory_order_acquire)),
            0U);
}

TEST_F(NonBlockingSlotTest, CloseDrainOpenHandsOffActiveSlot) {
  Slot* a = writer_->ActiveSlot();
  Slot* b = writer_->OtherSlot(a);

  SwitchResult r = writer_->CloseDrainOpen(a, b);
  ASSERT_EQ(r, SwitchResult::kSwitcher);

  // After Phase 3, active_buffer_ptr_ is B.  A is PENDING with count 0.
  ASSERT_EQ(writer_->ActiveSlot(), b);
  ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderState(
                a->header.load(std::memory_order_acquire)),
            uint64_t{NonBlockingIOWritableFileWriter::kBufferPending});
  ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderState(
                b->header.load(std::memory_order_acquire)),
            uint64_t{NonBlockingIOWritableFileWriter::kBufferActive});

  writer_->MarkBufferFree(a);
  ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderState(
                a->header.load(std::memory_order_acquire)),
            uint64_t{NonBlockingIOWritableFileWriter::kBufferFree});
}

TEST_F(NonBlockingSlotTest, SecondSwitcherReturnsNotSwitcher) {
  Slot* a = writer_->ActiveSlot();
  Slot* b = writer_->OtherSlot(a);

  // Winner fully completes the switch first, then the loser calls against
  // the same A.  A is no longer active, so Phase 1 sees the state mismatch.
  ASSERT_EQ(writer_->CloseDrainOpen(a, b), SwitchResult::kSwitcher);

  // The loser is about to call CloseDrainOpen(A, B) but B is now ACTIVE.
  // Its Phase 1 on A must see PENDING and return kNotSwitcher.  The wait
  // loop inside close_drain_open waits for active_buffer_ptr_ != A; it's
  // already != A because the winner published B, so the call returns
  // immediately.
  ASSERT_EQ(writer_->CloseDrainOpen(a, b), SwitchResult::kNotSwitcher);
}

// Race: N claimers vs. one closer.  The closer must wait for all claimants'
// releases before Phase 2 completes; no claimer ever lands in a PENDING slot.
TEST_F(NonBlockingSlotTest, ClaimersRaceAgainstCloser) {
  constexpr int kClaimers = 8;
  constexpr int kIters = 2000;

  std::atomic<int> ready{0};
  std::atomic<bool> start{false};
  std::atomic<int> successful_claims_on_a{0};
  std::atomic<int> successful_claims_on_b{0};
  Slot* a0 = writer_->ActiveSlot();
  Slot* b0 = writer_->OtherSlot(a0);

  std::vector<std::thread> threads;
  for (int t = 0; t < kClaimers; ++t) {
    threads.emplace_back([&]() {
      ready.fetch_add(1);
      while (!start.load(std::memory_order_acquire)) {
      }
      for (int i = 0; i < kIters; ++i) {
        Slot* s = writer_->ClaimSlot();
        // Claimed slot must be ACTIVE with count >= 1 at the moment of
        // observation.
        uint64_t h = s->header.load(std::memory_order_acquire);
        ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderState(h),
                  uint64_t{NonBlockingIOWritableFileWriter::kBufferActive});
        ASSERT_GE(NonBlockingIOWritableFileWriter::HeaderCount(h), 1U);
        if (s == a0) {
          successful_claims_on_a.fetch_add(1);
        } else if (s == b0) {
          successful_claims_on_b.fetch_add(1);
        }
        writer_->ReleaseSlot(s);
      }
    });
  }

  while (ready.load() < kClaimers) {
  }
  start.store(true, std::memory_order_release);

  // Let some claims pile up, then switch.  Do it twice to exercise A->B->A.
  for (int sw = 0; sw < 2; ++sw) {
    Slot* cur = writer_->ActiveSlot();
    Slot* other = writer_->OtherSlot(cur);
    SwitchResult r = writer_->CloseDrainOpen(cur, other);
    ASSERT_EQ(r, SwitchResult::kSwitcher);
    // After drain, count must be 0 on the just-closed slot.
    ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderCount(
                  cur->header.load(std::memory_order_acquire)),
              0U);
    writer_->MarkBufferFree(cur);
  }

  for (auto& th : threads) {
    th.join();
  }

  // Sanity: both slots saw traffic (otherwise the test would be vacuous).
  ASSERT_GT(successful_claims_on_a.load(), 0);
  ASSERT_GT(successful_claims_on_b.load(), 0);
  ASSERT_EQ(
      successful_claims_on_a.load() + successful_claims_on_b.load(),
      kClaimers * kIters);

  // All slots must be drained by now.
  ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderCount(
                writer_->Slot(0)->header.load(std::memory_order_acquire)),
            0U);
  ASSERT_EQ(NonBlockingIOWritableFileWriter::HeaderCount(
                writer_->Slot(1)->header.load(std::memory_order_acquire)),
            0U);
}

// Multiple threads race to close the same active slot.  Exactly one should
// return kSwitcher; all others must return kNotSwitcher.
TEST_F(NonBlockingSlotTest, OnlyOneSwitcherWinsPhaseOne) {
  constexpr int kRacers = 8;
  std::atomic<int> winners{0};
  std::atomic<int> losers{0};
  std::atomic<int> ready{0};
  std::atomic<bool> start{false};

  Slot* a = writer_->ActiveSlot();
  Slot* b = writer_->OtherSlot(a);

  std::vector<std::thread> threads;
  for (int t = 0; t < kRacers; ++t) {
    threads.emplace_back([&]() {
      ready.fetch_add(1);
      while (!start.load(std::memory_order_acquire)) {
      }
      SwitchResult r = writer_->CloseDrainOpen(a, b);
      if (r == SwitchResult::kSwitcher) {
        winners.fetch_add(1);
      } else {
        losers.fetch_add(1);
      }
    });
  }

  while (ready.load() < kRacers) {
  }
  start.store(true, std::memory_order_release);

  // Winner publishes B; to let the winner progress past Phase 3 even if
  // other threads haven't yet finished their wait_until_active_buffer_ptr
  // spin, we do nothing here — the winner doesn't block on losers.

  for (auto& th : threads) {
    th.join();
  }
  ASSERT_EQ(winners.load(), 1);
  ASSERT_EQ(losers.load(), kRacers - 1);
  ASSERT_EQ(writer_->ActiveSlot(), b);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
