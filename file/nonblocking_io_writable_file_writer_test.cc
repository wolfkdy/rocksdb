//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "file/nonblocking_io_writable_file_writer.h"

#include <atomic>
#include <chrono>
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

TEST_F(NonBlockingSlotTest, InitialActiveIsSlotZero) {
  Slot* a = writer_->ActiveSlot();
  Slot* b = writer_->OtherSlot(a);
  ASSERT_EQ(a, writer_->Slot(0));
  ASSERT_EQ(b, writer_->Slot(1));
  ASSERT_EQ(a->in_flight_count.load(), 0U);
  ASSERT_EQ(b->in_flight_count.load(), 0U);
  ASSERT_EQ(a->next_wal_offset, 0U);
  ASSERT_EQ(b->next_wal_offset, 0U);
}

TEST_F(NonBlockingSlotTest, ClaimAndReleaseAdjustsCount) {
  Slot* s1 = writer_->ClaimSlot();
  Slot* s2 = writer_->ClaimSlot();
  ASSERT_EQ(s1, s2);
  ASSERT_EQ(s1->in_flight_count.load(), 2U);
  writer_->ReleaseSlot(s1);
  ASSERT_EQ(s1->in_flight_count.load(), 1U);
  writer_->ReleaseSlot(s2);
  ASSERT_EQ(s1->in_flight_count.load(), 0U);
}

TEST_F(NonBlockingSlotTest, SwitchActivePublishesBAndCarriesOffset) {
  Slot* a = writer_->ActiveSlot();
  Slot* b = writer_->OtherSlot(a);
  // Simulate a writer reserving 1234 bytes in A.
  a->next_wal_offset = 1234;

  writer_->SwitchActive(a, b);
  ASSERT_EQ(writer_->ActiveSlot(), b);
  ASSERT_EQ(b->next_wal_offset, 0U);
  ASSERT_EQ(b->file_offset_start, 1234U);

  // Subsequent ClaimSlot lands in B.
  Slot* claimed = writer_->ClaimSlot();
  ASSERT_EQ(claimed, b);
  ASSERT_EQ(b->in_flight_count.load(), 1U);
  writer_->ReleaseSlot(claimed);
}

TEST_F(NonBlockingSlotTest, WaitDrainBlocksUntilInFlightZero) {
  Slot* a = writer_->ClaimSlot();
  Slot* b = writer_->OtherSlot(a);
  ASSERT_EQ(a->in_flight_count.load(), 1U);

  // Switch the active pointer to B; A is now closed but still has one
  // in-flight reservation.
  writer_->SwitchActive(a, b);

  std::atomic<bool> drained{false};
  std::thread drain_thread([&]() {
    writer_->WaitDrain(a);
    drained.store(true);
  });

  // Give the drain thread time to spin and confirm it stays blocked.
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  ASSERT_FALSE(drained.load());

  writer_->ReleaseSlot(a);
  drain_thread.join();
  ASSERT_TRUE(drained.load());
  ASSERT_EQ(a->in_flight_count.load(), 0U);
}

TEST_F(NonBlockingSlotTest, ResetDrainedSlotClearsOffset) {
  Slot* a = writer_->ActiveSlot();
  Slot* b = writer_->OtherSlot(a);
  a->next_wal_offset = 4096;
  writer_->SwitchActive(a, b);
  writer_->WaitDrain(a);
  writer_->ResetDrainedSlot(a);
  ASSERT_EQ(a->next_wal_offset, 0U);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
