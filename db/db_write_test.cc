//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <fstream>
#include <memory>
#include <thread>
#include <vector>

#include "db/db_test_util.h"
#include "db/write_batch_internal.h"
#include "db/write_thread.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

// Test variations of WriteImpl.
class DBWriteTest : public DBTestBase, public testing::WithParamInterface<int> {
 public:
  DBWriteTest() : DBTestBase("db_write_test", /*env_do_fsync=*/true) {}

  Options GetOptions() { return DBTestBase::GetOptions(GetParam()); }

  void Open() { DBTestBase::Reopen(GetOptions()); }
};

class DBWriteTestUnparameterized : public DBTestBase {
 public:
  explicit DBWriteTestUnparameterized()
      : DBTestBase("pipelined_write_test", /*env_do_fsync=*/false) {}
};

// It is invalid to do sync write while disabling WAL.
TEST_P(DBWriteTest, SyncAndDisableWAL) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = true;
  ASSERT_TRUE(dbfull()->Put(write_options, "foo", "bar").IsInvalidArgument());
  WriteBatch batch;
  ASSERT_OK(batch.Put("foo", "bar"));
  ASSERT_TRUE(dbfull()->Write(write_options, &batch).IsInvalidArgument());
}

TEST_P(DBWriteTest, WriteStallRemoveNoSlowdownWrite) {
  Options options = GetOptions();
  options.level0_stop_writes_trigger = options.level0_slowdown_writes_trigger =
      4;
  std::vector<port::Thread> threads;
  std::atomic<int> thread_num(0);
  port::Mutex mutex;
  port::CondVar cv(&mutex);
  // Guarded by mutex
  int writers = 0;

  Reopen(options);

  std::function<void()> write_slowdown_func = [&]() {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions wo;
    wo.no_slowdown = false;
    ASSERT_OK(dbfull()->Put(wo, key, "bar"));
  };
  std::function<void()> write_no_slowdown_func = [&]() {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions wo;
    wo.no_slowdown = true;
    Status s = dbfull()->Put(wo, key, "bar");
    ASSERT_TRUE(s.ok() || s.IsIncomplete());
  };
  std::function<void(void*)> unblock_main_thread_func = [&](void*) {
    mutex.Lock();
    ++writers;
    cv.SignalAll();
    mutex.Unlock();
  };

  // Create 3 L0 files and schedule 4th without waiting
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Start", unblock_main_thread_func);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteTest::WriteStallRemoveNoSlowdownWrite:1",
        "DBImpl::BackgroundCallFlush:start"},
       {"DBWriteTest::WriteStallRemoveNoSlowdownWrite:2",
        "DBImplWrite::PipelinedWriteImpl:AfterJoinBatchGroup"},
       // Make compaction start wait for the write stall to be detected and
       // implemented by a write group leader
       {"DBWriteTest::WriteStallRemoveNoSlowdownWrite:3",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Schedule creation of 4th L0 file without waiting. This will seal the
  // memtable and then wait for a sync point before writing the file. We need
  // to do it this way because SwitchMemtable() needs to enter the
  // write_thread
  FlushOptions fopt;
  fopt.wait = false;
  ASSERT_OK(dbfull()->Flush(fopt));

  // Create a mix of slowdown/no_slowdown write threads
  mutex.Lock();
  // First leader
  threads.emplace_back(write_slowdown_func);
  while (writers != 1) {
    cv.Wait();
  }

  // Second leader. Will stall writes
  // Build a writers list with no slowdown in the middle:
  //  +-------------+
  //  | slowdown    +<----+ newest
  //  +--+----------+
  //     |
  //     v
  //  +--+----------+
  //  | no slowdown |
  //  +--+----------+
  //     |
  //     v
  //  +--+----------+
  //  | slowdown    +
  //  +-------------+
  threads.emplace_back(write_slowdown_func);
  while (writers != 2) {
    cv.Wait();
  }
  threads.emplace_back(write_no_slowdown_func);
  while (writers != 3) {
    cv.Wait();
  }
  threads.emplace_back(write_slowdown_func);
  while (writers != 4) {
    cv.Wait();
  }

  mutex.Unlock();

  TEST_SYNC_POINT("DBWriteTest::WriteStallRemoveNoSlowdownWrite:1");
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(nullptr));
  // This would have triggered a write stall. Unblock the write group leader
  TEST_SYNC_POINT("DBWriteTest::WriteStallRemoveNoSlowdownWrite:2");
  // The leader is going to create missing newer links. When the leader
  // finishes, the next leader is going to delay writes and fail writers with
  // no_slowdown

  TEST_SYNC_POINT("DBWriteTest::WriteStallRemoveNoSlowdownWrite:3");
  for (auto& t : threads) {
    t.join();
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBWriteTest, WriteThreadHangOnWriteStall) {
  Options options = GetOptions();
  options.level0_stop_writes_trigger = options.level0_slowdown_writes_trigger = 4;
  std::vector<port::Thread> threads;
  std::atomic<int> thread_num(0);
  port::Mutex mutex;
  port::CondVar cv(&mutex);
  // Guarded by mutex
  int writers = 0;

  Reopen(options);

  std::function<void()> write_slowdown_func = [&]() {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions wo;
    wo.no_slowdown = false;
    ASSERT_OK(dbfull()->Put(wo, key, "bar"));
  };
  std::function<void()> write_no_slowdown_func = [&]() {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions wo;
    wo.no_slowdown = true;
    Status s = dbfull()->Put(wo, key, "bar");
    ASSERT_TRUE(s.ok() || s.IsIncomplete());
  };
  std::function<void(void *)> unblock_main_thread_func = [&](void *) {
    mutex.Lock();
    ++writers;
    cv.SignalAll();
    mutex.Unlock();
  };

  // Create 3 L0 files and schedule 4th without waiting
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Start", unblock_main_thread_func);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteTest::WriteThreadHangOnWriteStall:1",
        "DBImpl::BackgroundCallFlush:start"},
       {"DBWriteTest::WriteThreadHangOnWriteStall:2",
        "DBImpl::WriteImpl:BeforeLeaderEnters"},
       // Make compaction start wait for the write stall to be detected and
       // implemented by a write group leader
       {"DBWriteTest::WriteThreadHangOnWriteStall:3",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Schedule creation of 4th L0 file without waiting. This will seal the
  // memtable and then wait for a sync point before writing the file. We need
  // to do it this way because SwitchMemtable() needs to enter the
  // write_thread
  FlushOptions fopt;
  fopt.wait = false;
  ASSERT_OK(dbfull()->Flush(fopt));

  // Create a mix of slowdown/no_slowdown write threads
  mutex.Lock();
  // First leader
  threads.emplace_back(write_slowdown_func);
  while (writers != 1) {
    cv.Wait();
  }
  // Second leader. Will stall writes
  threads.emplace_back(write_slowdown_func);
  threads.emplace_back(write_no_slowdown_func);
  threads.emplace_back(write_slowdown_func);
  threads.emplace_back(write_no_slowdown_func);
  threads.emplace_back(write_slowdown_func);
  while (writers != 6) {
    cv.Wait();
  }
  mutex.Unlock();

  TEST_SYNC_POINT("DBWriteTest::WriteThreadHangOnWriteStall:1");
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(nullptr));
  // This would have triggered a write stall. Unblock the write group leader
  TEST_SYNC_POINT("DBWriteTest::WriteThreadHangOnWriteStall:2");
  // The leader is going to create missing newer links. When the leader finishes,
  // the next leader is going to delay writes and fail writers with no_slowdown

  TEST_SYNC_POINT("DBWriteTest::WriteThreadHangOnWriteStall:3");
  for (auto& t : threads) {
    t.join();
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBWriteTest, IOErrorOnWALWritePropagateToWriteThreadFollower) {
  constexpr int kNumThreads = 5;
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetOptions();
  options.env = mock_env.get();
  Reopen(options);
  std::atomic<int> ready_count{0};
  std::atomic<int> leader_count{0};
  std::vector<port::Thread> threads;
  mock_env->SetFilesystemActive(false);

  // Wait until all threads linked to write threads, to make sure
  // all threads join the same batch group.
  SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
        ready_count++;
        auto* w = reinterpret_cast<WriteThread::Writer*>(arg);
        if (w->state == WriteThread::STATE_GROUP_LEADER) {
          leader_count++;
          while (ready_count < kNumThreads) {
            // busy waiting
          }
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < kNumThreads; i++) {
    threads.push_back(port::Thread(
        [&](int index) {
          // All threads should fail.
          auto res = Put("key" + std::to_string(index), "value");
          if (options.manual_wal_flush) {
            ASSERT_TRUE(res.ok());
            // we should see fs error when we do the flush

            // TSAN reports a false alarm for lock-order-inversion but Open and
            // FlushWAL are not run concurrently. Disabling this until TSAN is
            // fixed.
            // res = dbfull()->FlushWAL(false);
            // ASSERT_FALSE(res.ok());
          } else {
            ASSERT_FALSE(res.ok());
          }
        },
        i));
  }
  for (int i = 0; i < kNumThreads; i++) {
    threads[i].join();
  }
  ASSERT_EQ(1, leader_count);

  // The Failed PUT operations can cause a BG error to be set.
  // Mark it as Checked for the ASSERT_STATUS_CHECKED
  dbfull()->Resume().PermitUncheckedError();

  // Close before mock_env destruct.
  Close();
}

TEST_F(DBWriteTestUnparameterized, PipelinedWriteRace) {
  // This test was written to trigger a race in ExitAsBatchGroupLeader in case
  // enable_pipelined_write_ was true.
  // Writers for which ShouldWriteToMemtable() evaluates to false are removed
  // from the write_group via CompleteFollower/ CompleteLeader. Writers in the
  // middle of the group are fully unlinked, but if that writers is the
  // last_writer, then we did not update the predecessor's link_older, i.e.,
  // this writer was still reachable via newest_writer_.
  //
  // But the problem was, that CompleteFollower already wakes up the thread
  // owning that writer before the writer has been removed. This resulted in a
  // race - if the leader thread was fast enough, then everything was fine.
  // However, if the woken up thread finished the current write operation and
  // then performed yet another write, then a new writer instance was added
  // to newest_writer_. It is possible that the new writer is located on the
  // same address on stack, and if this happened, then we had a problem,
  // because the old code tried to find the last_writer in the list to unlink
  // it, which in this case produced a cycle in the list.
  // Whether two invocations of PipelinedWriteImpl() by the same thread actually
  // allocate the writer on the same address depends on the OS and/or compiler,
  // so it is rather hard to create a deterministic test for this.

  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_pipelined_write = true;
  std::vector<port::Thread> threads;

  std::atomic<int> write_counter{0};
  std::atomic<int> active_writers{0};
  std::atomic<bool> second_write_starting{false};
  std::atomic<bool> second_write_in_progress{false};
  std::atomic<WriteThread::Writer*> leader{nullptr};
  std::atomic<bool> finished_WAL_write{false};

  DestroyAndReopen(options);

  auto write_one_doc = [&]() {
    int a = write_counter.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions wo;
    ASSERT_OK(dbfull()->Put(wo, key, "bar"));
    --active_writers;
  };

  auto write_two_docs = [&]() {
    write_one_doc();
    second_write_starting = true;
    write_one_doc();
  };

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
        if (second_write_starting.load()) {
          second_write_in_progress = true;
          return;
        }
        auto* w = reinterpret_cast<WriteThread::Writer*>(arg);
        if (w->state == WriteThread::STATE_GROUP_LEADER) {
          active_writers++;
          if (leader.load() == nullptr) {
            leader.store(w);
            while (active_writers.load() < 2) {
              // wait for another thread to join the write_group
            }
          }
        } else {
          // we disable the memtable for all followers so that they they are
          // removed from the write_group before enqueuing it for the memtable
          // write
          w->disable_memtable = true;
          active_writers++;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::ExitAsBatchGroupLeader:Start", [&](void* arg) {
        auto* wg = reinterpret_cast<WriteThread::WriteGroup*>(arg);
        if (wg->leader == leader && !finished_WAL_write) {
          finished_WAL_write = true;
          while (active_writers.load() < 3) {
            // wait for the new writer to be enqueued
          }
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::ExitAsBatchGroupLeader:AfterCompleteWriters",
      [&](void* arg) {
        auto* wg = reinterpret_cast<WriteThread::WriteGroup*>(arg);
        if (wg->leader == leader) {
          while (!second_write_in_progress.load()) {
            // wait for the old follower thread to start the next write
          }
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // start leader + one follower
  threads.emplace_back(write_one_doc);
  while (leader.load() == nullptr) {
    // wait for leader
  }

  // we perform two writes in the follower, so that for the second write
  // the thread reinserts a Writer with the same address
  threads.emplace_back(write_two_docs);

  // wait for the leader to enter ExitAsBatchGroupLeader
  while (!finished_WAL_write.load()) {
    // wait for write_group to have finished the WAL writes
  }

  // start another writer thread to be enqueued before the leader can
  // complete the writers from its write_group
  threads.emplace_back(write_one_doc);

  for (auto& t : threads) {
    t.join();
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBWriteTest, ManualWalFlushInEffect) {
  Options options = GetOptions();
  Reopen(options);
  // try the 1st WAL created during open
  ASSERT_TRUE(Put("key" + std::to_string(0), "value").ok());
  ASSERT_TRUE(options.manual_wal_flush != dbfull()->TEST_WALBufferIsEmpty());
  ASSERT_TRUE(dbfull()->FlushWAL(false).ok());
  ASSERT_TRUE(dbfull()->TEST_WALBufferIsEmpty());
  // try the 2nd wal created during SwitchWAL
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  ASSERT_TRUE(Put("key" + std::to_string(0), "value").ok());
  ASSERT_TRUE(options.manual_wal_flush != dbfull()->TEST_WALBufferIsEmpty());
  ASSERT_TRUE(dbfull()->FlushWAL(false).ok());
  ASSERT_TRUE(dbfull()->TEST_WALBufferIsEmpty());
}

TEST_P(DBWriteTest, UnflushedPutRaceWithTrackedWalSync) {
  // Repro race condition bug where unflushed WAL data extended the synced size
  // recorded to MANIFEST despite being unrecoverable.
  Options options = GetOptions();
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  options.env = fault_env.get();
  options.manual_wal_flush = true;
  options.track_and_verify_wals_in_manifest = true;
  Reopen(options);

  ASSERT_OK(Put("key1", "val1"));

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWAL:Begin",
      [this](void* /* arg */) { ASSERT_OK(Put("key2", "val2")); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->FlushWAL(true /* sync */));

  // Ensure callback ran.
  ASSERT_EQ("val2", Get("key2"));

  Close();

  // Simulate full loss of unsynced data. This drops "key2" -> "val2" from the
  // DB WAL.
  fault_env->DropUnsyncedFileData();

  Reopen(options);

  // Need to close before `fault_env` goes out of scope.
  Close();
}

TEST_P(DBWriteTest, InactiveWalFullySyncedBeforeUntracked) {
  // Repro bug where a WAL is appended and switched after
  // `FlushWAL(true /* sync */)`'s sync finishes and before it untracks fully
  // synced inactive logs. Previously such a WAL would be wrongly untracked
  // so the final append would never be synced.
  Options options = GetOptions();
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  options.env = fault_env.get();
  Reopen(options);

  ASSERT_OK(Put("key1", "val1"));

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWAL:BeforeMarkLogsSynced:1", [this](void* /* arg */) {
        ASSERT_OK(Put("key2", "val2"));
        ASSERT_OK(dbfull()->TEST_SwitchMemtable());
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->FlushWAL(true /* sync */));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(Put("key3", "val3"));

  ASSERT_OK(db_->FlushWAL(true /* sync */));

  Close();

  // Simulate full loss of unsynced data. This should drop nothing since we did
  // `FlushWAL(true /* sync */)` before `Close()`.
  fault_env->DropUnsyncedFileData();

  Reopen(options);

  ASSERT_EQ("val1", Get("key1"));
  ASSERT_EQ("val2", Get("key2"));
  ASSERT_EQ("val3", Get("key3"));

  // Need to close before `fault_env` goes out of scope.
  Close();
}

TEST_P(DBWriteTest, IOErrorOnWALWriteTriggersReadOnlyMode) {
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetOptions();
  options.env = mock_env.get();
  Reopen(options);
  for (int i = 0; i < 2; i++) {
    // Forcibly fail WAL write for the first Put only. Subsequent Puts should
    // fail due to read-only mode
    mock_env->SetFilesystemActive(i != 0);
    auto res = Put("key" + std::to_string(i), "value");
    // TSAN reports a false alarm for lock-order-inversion but Open and
    // FlushWAL are not run concurrently. Disabling this until TSAN is
    // fixed.
    /*
    if (options.manual_wal_flush && i == 0) {
      // even with manual_wal_flush the 2nd Put should return error because of
      // the read-only mode
      ASSERT_TRUE(res.ok());
      // we should see fs error when we do the flush
      res = dbfull()->FlushWAL(false);
    }
    */
    if (!options.manual_wal_flush) {
      ASSERT_NOK(res);
    } else {
      ASSERT_OK(res);
    }
  }
  // Close before mock_env destruct.
  Close();
}

TEST_P(DBWriteTest, IOErrorOnSwitchMemtable) {
  Random rnd(301);
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetOptions();
  options.env = mock_env.get();
  options.writable_file_max_buffer_size = 4 * 1024 * 1024;
  options.write_buffer_size = 3 * 512 * 1024;
  options.wal_bytes_per_sync = 256 * 1024;
  options.manual_wal_flush = true;
  Reopen(options);
  mock_env->SetFilesystemActive(false, Status::IOError("Not active"));
  Status s;
  for (int i = 0; i < 4 * 512; ++i) {
    s = Put(Key(i), rnd.RandomString(1024));
    if (!s.ok()) {
      break;
    }
  }
  ASSERT_EQ(s.severity(), Status::Severity::kFatalError);

  mock_env->SetFilesystemActive(true);
  // Close before mock_env destruct.
  Close();
}

// Test that db->LockWAL() flushes the WAL after locking.
TEST_P(DBWriteTest, LockWalInEffect) {
  Options options = GetOptions();
  Reopen(options);
  // try the 1st WAL created during open
  ASSERT_OK(Put("key" + std::to_string(0), "value"));
  ASSERT_TRUE(options.manual_wal_flush != dbfull()->TEST_WALBufferIsEmpty());
  ASSERT_OK(dbfull()->LockWAL());
  ASSERT_TRUE(dbfull()->TEST_WALBufferIsEmpty(false));
  ASSERT_OK(dbfull()->UnlockWAL());
  // try the 2nd wal created during SwitchWAL
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  ASSERT_OK(Put("key" + std::to_string(0), "value"));
  ASSERT_TRUE(options.manual_wal_flush != dbfull()->TEST_WALBufferIsEmpty());
  ASSERT_OK(dbfull()->LockWAL());
  ASSERT_TRUE(dbfull()->TEST_WALBufferIsEmpty(false));
  ASSERT_OK(dbfull()->UnlockWAL());
}

TEST_P(DBWriteTest, ConcurrentlyDisabledWAL) {
    Options options = GetOptions();
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.statistics->set_stats_level(StatsLevel::kAll);
    Reopen(options);
    std::string wal_key_prefix = "WAL_KEY_";
    std::string no_wal_key_prefix = "K_";
    // 100 KB value each for NO-WAL operation
    std::string no_wal_value(1024 * 100, 'X');
    // 1B value each for WAL operation
    std::string wal_value = "0";
    std::thread threads[10];
    for (int t = 0; t < 10; t++) {
        threads[t] = std::thread([t, wal_key_prefix, wal_value, no_wal_key_prefix, no_wal_value, this] {
            for(int i = 0; i < 10; i++) {
              ROCKSDB_NAMESPACE::WriteOptions write_option_disable;
              write_option_disable.disableWAL = true;
              ROCKSDB_NAMESPACE::WriteOptions write_option_default;
              std::string no_wal_key = no_wal_key_prefix + std::to_string(t) +
                                       "_" + std::to_string(i);
              ASSERT_OK(
                  this->Put(no_wal_key, no_wal_value, write_option_disable));
              std::string wal_key =
                  wal_key_prefix + std::to_string(i) + "_" + std::to_string(i);
              ASSERT_OK(this->Put(wal_key, wal_value, write_option_default));
              ASSERT_OK(dbfull()->SyncWAL());
            }
            return;
        });
    }
    for (auto& t: threads) {
        t.join();
    }
    uint64_t bytes_num = options.statistics->getTickerCount(
        ROCKSDB_NAMESPACE::Tickers::WAL_FILE_BYTES);
    // written WAL size should less than 100KB (even included HEADER & FOOTER overhead)
    ASSERT_LE(bytes_num, 1024 * 100);
}

// Tests for NonBlockingIOWritableFileWriter (nonblocking_wal_io mode).
class DBWriteNonBlockingIOTest : public DBTestBase {
 public:
  DBWriteNonBlockingIOTest()
      : DBTestBase("db_write_nonblocking_io_test", /*env_do_fsync=*/true) {}
};

// Verify that writes can proceed concurrently while SyncWAL is writing
// the swapped buffer to disk.
TEST_F(DBWriteNonBlockingIOTest, ConcurrentWriteDuringSyncWAL) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", "val1"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::SyncWALNonBlockingIO:AfterSwapBuffer",
       "DBWriteNonBlockingIOTest::ConcurrentWriteDuringSyncWAL:DoPut"},
      {"DBWriteNonBlockingIOTest::ConcurrentWriteDuringSyncWAL:PutDone",
       "DBImpl::SyncWALNonBlockingIO:AfterFlushAndSync"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread sync_thread([&]() { ASSERT_OK(db_->SyncWAL()); });

  TEST_SYNC_POINT(
      "DBWriteNonBlockingIOTest::ConcurrentWriteDuringSyncWAL:DoPut");
  ASSERT_OK(Put("key2", "val2"));
  TEST_SYNC_POINT(
      "DBWriteNonBlockingIOTest::ConcurrentWriteDuringSyncWAL:PutDone");

  sync_thread.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // Sync the second Put's data.
  ASSERT_OK(db_->SyncWAL());

  ASSERT_EQ("val1", Get("key1"));
  ASSERT_EQ("val2", Get("key2"));
}

// Verify that SwitchMemtable waits for an in-progress SyncWAL to finish
// before calling WriteBuffer() on the same log.
TEST_F(DBWriteNonBlockingIOTest, SwitchMemtableWaitsForSyncWAL) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", "val1"));

  std::atomic<bool> sync_flush_done{false};

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWALNonBlockingIO:AfterFlushAndSync",
      [&](void*) { sync_flush_done.store(true); });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::SyncWALNonBlockingIO:AfterSwapBuffer",
       "DBWriteNonBlockingIOTest::SwitchMemtableWaitsForSyncWAL:StartSwitch"},
      {"DBWriteNonBlockingIOTest::SwitchMemtableWaitsForSyncWAL:SwitchStarted",
       "DBImpl::SyncWALNonBlockingIO:AfterFlushAndSync"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread sync_thread([&]() { ASSERT_OK(db_->SyncWAL()); });

  TEST_SYNC_POINT(
      "DBWriteNonBlockingIOTest::SwitchMemtableWaitsForSyncWAL:StartSwitch");

  port::Thread switch_thread([&]() {
    ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    // SwitchMemtable's WriteBuffer() must not proceed until SyncWAL finishes.
    ASSERT_TRUE(sync_flush_done.load());
  });

  env_->SleepForMicroseconds(100000);  // 100ms

  TEST_SYNC_POINT(
      "DBWriteNonBlockingIOTest::SwitchMemtableWaitsForSyncWAL:SwitchStarted");

  sync_thread.join();
  switch_thread.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(db_->SyncWAL());
  ASSERT_EQ("val1", Get("key1"));
}

// Verify data durability: after SyncWAL + Close + reopen, all data is
// recovered.
TEST_F(DBWriteNonBlockingIOTest, RecoveryAfterSyncWAL) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  DestroyAndReopen(options);

  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put("key" + std::to_string(i), "val" + std::to_string(i)));
  }

  ASSERT_OK(db_->SyncWAL());
  Close();
  Reopen(options);

  for (int i = 0; i < 100; i++) {
    ASSERT_EQ("val" + std::to_string(i), Get("key" + std::to_string(i)));
  }
}

// Stress test: many threads writing concurrently with periodic SyncWAL.
TEST_F(DBWriteNonBlockingIOTest, StressWriteWithSyncWAL) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  DestroyAndReopen(options);

  const int kNumWriters = 4;
  const int kWritesPerThread = 200;
  std::atomic<int> errors{0};

  std::vector<port::Thread> writers;
  for (int t = 0; t < kNumWriters; t++) {
    writers.emplace_back([&, t]() {
      for (int i = 0; i < kWritesPerThread; i++) {
        auto s = Put("t" + std::to_string(t) + "_k" + std::to_string(i),
                      "val" + std::to_string(i));
        if (!s.ok()) errors.fetch_add(1);
      }
    });
  }

  std::atomic<bool> stop_syncer{false};
  port::Thread syncer([&]() {
    while (!stop_syncer.load()) {
      auto s = db_->SyncWAL();
      if (!s.ok()) errors.fetch_add(1);
    }
  });

  for (auto& w : writers) {
    w.join();
  }
  stop_syncer.store(true);
  syncer.join();

  ASSERT_EQ(0, errors.load());

  ASSERT_OK(db_->SyncWAL());

  for (int t = 0; t < kNumWriters; t++) {
    for (int i = 0; i < kWritesPerThread; i++) {
      ASSERT_EQ("val" + std::to_string(i),
                Get("t" + std::to_string(t) + "_k" + std::to_string(i)));
    }
  }
}

// Verify that nonblocking_wal_io is incompatible with manual_wal_flush.
TEST_F(DBWriteNonBlockingIOTest, IncompatibleWithManualWalFlush) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  options.manual_wal_flush = true;
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// Verify that nonblocking_wal_io is incompatible with two_write_queues.
TEST_F(DBWriteNonBlockingIOTest, IncompatibleWithTwoWriteQueues) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  options.two_write_queues = true;
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// Verify that WriteOptions::sync=true is rejected with nonblocking_wal_io.
TEST_F(DBWriteNonBlockingIOTest, SyncWriteNotSupported) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  DestroyAndReopen(options);

  WriteOptions wo;
  wo.sync = true;
  Status s = db_->Put(wo, "key", "val");
  ASSERT_TRUE(s.IsNotSupported());

  // Normal writes and explicit SyncWAL should still work.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(db_->SyncWAL());
  ASSERT_EQ("val", Get("key"));
}

// Race: SyncWAL #1 is in the flush phase (IsSyncing=true, no lock), a second
// SyncWAL arrives and must wait.  Meanwhile a writer appends into the fresh
// buf_.  When SyncWAL #1 finishes, SyncWAL #2 proceeds and picks up the
// writer's new data.
TEST_F(DBWriteNonBlockingIOTest, ConcurrentSyncWAL) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", "val1"));

  // Force this interleaving:
  //   SyncWAL-1: SwapBuffer -> [pause] -> FlushAndSync -> done
  //   SyncWAL-2:                enter -> wait(IsSyncing) -> SwapBuffer -> ...
  //   Writer:                   Put("key2") during SyncWAL-1 flush phase
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      // SyncWAL-1 swapped, now let writer and SyncWAL-2 start.
      {"DBImpl::SyncWALNonBlockingIO:AfterSwapBuffer",
       "ConcurrentSyncWAL:StartWriterAndSync2"},
      // Hold SyncWAL-1's flush until writer and SyncWAL-2 are in flight.
      {"ConcurrentSyncWAL:WriterAndSync2Started",
       "DBImpl::SyncWALNonBlockingIO:AfterFlushAndSync"},
  });

  std::atomic<bool> sync2_entered{false};

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // SyncWAL-1: will pause after SwapBuffer.
  port::Thread sync1([&]() { ASSERT_OK(db_->SyncWAL()); });

  TEST_SYNC_POINT("ConcurrentSyncWAL:StartWriterAndSync2");

  // Writer: appends into the fresh buf_ while SyncWAL-1 flushes flush_buf_.
  ASSERT_OK(Put("key2", "val2"));

  // SyncWAL-2: enters and must wait on IsSyncing set by SyncWAL-1.
  port::Thread sync2([&]() {
    sync2_entered.store(true);
    ASSERT_OK(db_->SyncWAL());
  });

  // Give sync2 a moment to actually enter and block on IsSyncing.
  while (!sync2_entered.load()) {
    env_->SleepForMicroseconds(1000);
  }
  env_->SleepForMicroseconds(10000);

  TEST_SYNC_POINT("ConcurrentSyncWAL:WriterAndSync2Started");

  sync1.join();
  sync2.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ("val1", Get("key1"));
  ASSERT_EQ("val2", Get("key2"));

  // key2 was synced by SyncWAL-2, verify durability.
  Close();
  Reopen(options);
  ASSERT_EQ("val1", Get("key1"));
  ASSERT_EQ("val2", Get("key2"));
}

// Race: LockWAL arrives while SyncWAL is in the flush phase.
// LockWAL must wait for !IsSyncing before calling WriteBuffer().
// Verify no concurrent writable_file_->Append() calls.
TEST_F(DBWriteNonBlockingIOTest, LockWALWaitsForSyncWAL) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", "val1"));

  std::atomic<bool> sync_flush_done{false};

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWALNonBlockingIO:AfterFlushAndSync",
      [&](void*) { sync_flush_done.store(true); });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      // SyncWAL swapped, let LockWAL proceed (it should block on IsSyncing).
      {"DBImpl::SyncWALNonBlockingIO:AfterSwapBuffer",
       "LockWALWaitsForSyncWAL:StartLock"},
      // Hold SyncWAL's flush until LockWAL has had time to reach the wait.
      {"LockWALWaitsForSyncWAL:LockStarted",
       "DBImpl::SyncWALNonBlockingIO:AfterFlushAndSync"},
  });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread sync_thread([&]() { ASSERT_OK(db_->SyncWAL()); });

  TEST_SYNC_POINT("LockWALWaitsForSyncWAL:StartLock");

  port::Thread lock_thread([&]() {
    ASSERT_OK(db_->LockWAL());
    // When LockWAL returns, SyncWAL's flush phase must have completed.
    ASSERT_TRUE(sync_flush_done.load());
    ASSERT_OK(db_->UnlockWAL());
  });

  env_->SleepForMicroseconds(50000);  // Let LockWAL block on IsSyncing.
  TEST_SYNC_POINT("LockWALWaitsForSyncWAL:LockStarted");

  sync_thread.join();
  lock_thread.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ("val1", Get("key1"));
}

// Race: SwitchMemtable (via Flush) arrives while SyncWAL is in the flush
// phase.  Forces the exact interleaving where SwitchMemtable must wait for
// !IsSyncing before calling WriteBuffer() on the outgoing log.
TEST_F(DBWriteNonBlockingIOTest, FlushWaitsForSyncWAL) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", "val1"));

  std::atomic<bool> sync_flush_done{false};

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWALNonBlockingIO:AfterFlushAndSync",
      [&](void*) { sync_flush_done.store(true); });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::SyncWALNonBlockingIO:AfterSwapBuffer",
       "FlushWaitsForSyncWAL:StartFlush"},
      {"FlushWaitsForSyncWAL:FlushStarted",
       "DBImpl::SyncWALNonBlockingIO:AfterFlushAndSync"},
  });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread sync_thread([&]() { ASSERT_OK(db_->SyncWAL()); });

  TEST_SYNC_POINT("FlushWaitsForSyncWAL:StartFlush");

  port::Thread flush_thread([&]() {
    ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    // SwitchMemtable's WriteBuffer must not run until SyncWAL finishes flush.
    ASSERT_TRUE(sync_flush_done.load());
  });

  env_->SleepForMicroseconds(50000);  // Let SwitchMemtable block on IsSyncing.
  TEST_SYNC_POINT("FlushWaitsForSyncWAL:FlushStarted");

  sync_thread.join();
  flush_thread.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(db_->SyncWAL());
  ASSERT_EQ("val1", Get("key1"));
}

// Stress: writers + SyncWAL + Flush hammer the system for many rounds.
// Small write_buffer_size forces frequent SwitchMemtable, exercising
// SyncClosedLogs vs SyncWALNonBlockingIO on overlapping log ranges.
// Verify recovery after reopen.
TEST_F(DBWriteNonBlockingIOTest, StressWriteSyncFlushRecovery) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  options.write_buffer_size = 4096;
  DestroyAndReopen(options);

  const int kNumWriters = 4;
  const int kWritesPerThread = 500;
  std::atomic<int> errors{0};
  std::atomic<bool> stop{false};

  std::vector<port::Thread> writers;
  for (int t = 0; t < kNumWriters; t++) {
    writers.emplace_back([&, t]() {
      for (int i = 0; i < kWritesPerThread; i++) {
        auto s = Put("t" + std::to_string(t) + "_" + std::to_string(i),
                      "v" + std::to_string(i));
        if (!s.ok()) errors.fetch_add(1);
      }
    });
  }

  port::Thread syncer([&]() {
    while (!stop.load()) {
      auto s = db_->SyncWAL();
      if (!s.ok()) errors.fetch_add(1);
    }
  });

  port::Thread flusher([&]() {
    while (!stop.load()) {
      db_->Flush(FlushOptions());
    }
  });

  port::Thread locker([&]() {
    while (!stop.load()) {
      auto s = db_->LockWAL();
      if (s.ok()) {
        db_->UnlockWAL();
      }
    }
  });

  for (auto& w : writers) {
    w.join();
  }
  stop.store(true);
  syncer.join();
  flusher.join();
  locker.join();

  ASSERT_EQ(0, errors.load());
  ASSERT_OK(db_->SyncWAL());

  // Verify all data before reopen.
  for (int t = 0; t < kNumWriters; t++) {
    for (int i = 0; i < kWritesPerThread; i++) {
      ASSERT_EQ("v" + std::to_string(i),
                Get("t" + std::to_string(t) + "_" + std::to_string(i)));
    }
  }

  // Verify WAL recovery.
  Close();
  Reopen(options);
  for (int t = 0; t < kNumWriters; t++) {
    for (int i = 0; i < kWritesPerThread; i++) {
      ASSERT_EQ("v" + std::to_string(i),
                Get("t" + std::to_string(t) + "_" + std::to_string(i)));
    }
  }
}

// Fuzz test: writers + multiple SyncWAL threads hammer the system for 10s.
// No sync points — relies on real scheduling to expose races.
// TSAN/ASAN builds are the primary target.
TEST_F(DBWriteNonBlockingIOTest, FuzzWriteAndSyncWAL) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  DestroyAndReopen(options);

  const int kDurationSec = 20;
  std::atomic<bool> stop{false};
  std::atomic<int> errors{0};
  std::atomic<uint64_t> write_count{0};

  // 4 writer threads, continuous Put with varying value sizes.
  const int kNumWriters = 4;
  std::vector<port::Thread> writers;
  for (int t = 0; t < kNumWriters; t++) {
    writers.emplace_back([&, t]() {
      int i = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        // Vary value size to exercise buffer growth.
        std::string val(100 + (i % 500), 'a' + (t % 26));
        auto s = Put("fw" + std::to_string(t) + "_" + std::to_string(i), val);
        if (!s.ok()) errors.fetch_add(1);
        write_count.fetch_add(1, std::memory_order_relaxed);
        i++;
      }
    });
  }

  // 2 SyncWAL threads racing with each other.
  const int kNumSyncers = 2;
  std::vector<port::Thread> syncers;
  for (int t = 0; t < kNumSyncers; t++) {
    syncers.emplace_back([&]() {
      while (!stop.load(std::memory_order_relaxed)) {
        auto s = db_->SyncWAL();
        if (!s.ok()) errors.fetch_add(1);
      }
    });
  }

  env_->SleepForMicroseconds(kDurationSec * 1000000);
  stop.store(true);

  for (auto& w : writers) w.join();
  for (auto& s : syncers) s.join();

  ASSERT_EQ(0, errors.load());
  fprintf(stderr, "FuzzWriteAndSyncWAL: %" PRIu64 " writes in %ds\n",
          write_count.load(), kDurationSec);
}

// Fuzz test: writers + SyncWAL + LockWAL/UnlockWAL race for 10s.
// Corresponds to LockWALWaitsForSyncWAL sync point test.
TEST_F(DBWriteNonBlockingIOTest, FuzzWriteSyncLock) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  DestroyAndReopen(options);

  const int kDurationSec = 20;
  std::atomic<bool> stop{false};
  std::atomic<int> errors{0};
  std::atomic<uint64_t> write_count{0};

  const int kNumWriters = 4;
  std::vector<port::Thread> writers;
  for (int t = 0; t < kNumWriters; t++) {
    writers.emplace_back([&, t]() {
      int i = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        auto s = Put("wl" + std::to_string(t) + "_" + std::to_string(i),
                      std::string(100, 'y'));
        if (!s.ok()) errors.fetch_add(1);
        write_count.fetch_add(1, std::memory_order_relaxed);
        i++;
      }
    });
  }

  port::Thread syncer([&]() {
    while (!stop.load(std::memory_order_relaxed)) {
      auto s = db_->SyncWAL();
      if (!s.ok()) errors.fetch_add(1);
    }
  });

  port::Thread locker([&]() {
    while (!stop.load(std::memory_order_relaxed)) {
      auto s = db_->LockWAL();
      if (s.ok()) {
        db_->UnlockWAL();
      }
    }
  });

  env_->SleepForMicroseconds(kDurationSec * 1000000);
  stop.store(true);

  for (auto& w : writers) w.join();
  syncer.join();
  locker.join();

  ASSERT_EQ(0, errors.load());
  fprintf(stderr, "FuzzWriteSyncLock: %" PRIu64 " writes in %ds\n",
          write_count.load(), kDurationSec);
}

// Fuzz test: writers + SyncWAL + Flush race for 10s.
// Small write_buffer_size forces frequent SwitchMemtable.
// Corresponds to FlushWaitsForSyncWAL sync point test.
TEST_F(DBWriteNonBlockingIOTest, FuzzWriteSyncFlush) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  options.write_buffer_size = 4096;
  DestroyAndReopen(options);

  const int kDurationSec = 20;
  std::atomic<bool> stop{false};
  std::atomic<int> errors{0};
  std::atomic<uint64_t> write_count{0};

  const int kNumWriters = 4;
  std::vector<port::Thread> writers;
  for (int t = 0; t < kNumWriters; t++) {
    writers.emplace_back([&, t]() {
      int i = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        auto s = Put("wf" + std::to_string(t) + "_" + std::to_string(i),
                      std::string(100, 'z'));
        if (!s.ok()) errors.fetch_add(1);
        write_count.fetch_add(1, std::memory_order_relaxed);
        i++;
      }
    });
  }

  port::Thread syncer([&]() {
    while (!stop.load(std::memory_order_relaxed)) {
      auto s = db_->SyncWAL();
      if (!s.ok()) errors.fetch_add(1);
    }
  });

  port::Thread flusher([&]() {
    while (!stop.load(std::memory_order_relaxed)) {
      db_->Flush(FlushOptions());
    }
  });

  env_->SleepForMicroseconds(kDurationSec * 1000000);
  stop.store(true);

  for (auto& w : writers) w.join();
  syncer.join();
  flusher.join();

  ASSERT_EQ(0, errors.load());
  fprintf(stderr, "FuzzWriteSyncFlush: %" PRIu64 " writes in %ds\n",
          write_count.load(), kDurationSec);
}

// Fuzz test: writers + SyncWAL + Flush + LockWAL all race for 10s.
// Small write_buffer_size forces frequent SwitchMemtable and SyncClosedLogs.
TEST_F(DBWriteNonBlockingIOTest, FuzzWriteSyncFlushLock) {
  Options options = CurrentOptions();
  options.nonblocking_wal_io = true;
  options.write_buffer_size = 4096;
  DestroyAndReopen(options);

  const int kDurationSec = 20;
  std::atomic<bool> stop{false};
  std::atomic<int> errors{0};
  std::atomic<uint64_t> write_count{0};

  // 4 writer threads.
  const int kNumWriters = 4;
  std::vector<port::Thread> writers;
  for (int t = 0; t < kNumWriters; t++) {
    writers.emplace_back([&, t]() {
      int i = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        auto s = Put("fl" + std::to_string(t) + "_" + std::to_string(i),
                      std::string(100, 'x'));
        if (!s.ok()) errors.fetch_add(1);
        write_count.fetch_add(1, std::memory_order_relaxed);
        i++;
      }
    });
  }

  // SyncWAL thread.
  port::Thread syncer([&]() {
    while (!stop.load(std::memory_order_relaxed)) {
      auto s = db_->SyncWAL();
      if (!s.ok()) errors.fetch_add(1);
    }
  });

  // Flush thread — forces SwitchMemtable + SyncClosedLogs.
  port::Thread flusher([&]() {
    while (!stop.load(std::memory_order_relaxed)) {
      db_->Flush(FlushOptions());
    }
  });

  // LockWAL/UnlockWAL thread.
  port::Thread locker([&]() {
    while (!stop.load(std::memory_order_relaxed)) {
      auto s = db_->LockWAL();
      if (s.ok()) {
        db_->UnlockWAL();
      }
    }
  });

  env_->SleepForMicroseconds(kDurationSec * 1000000);
  stop.store(true);

  for (auto& w : writers) w.join();
  syncer.join();
  flusher.join();
  locker.join();

  ASSERT_EQ(0, errors.load());
  fprintf(stderr, "FuzzWriteSyncFlushLock: %" PRIu64 " writes in %ds\n",
          write_count.load(), kDurationSec);

  // Final SyncWAL + reopen to verify WAL consistency.
  ASSERT_OK(db_->SyncWAL());
  Close();
  Reopen(options);
  // Spot-check: all keys from writer 0 must be readable.
  for (int i = 0;; i++) {
    auto val = Get("fl0_" + std::to_string(i));
    if (val == "NOT_FOUND") break;
    ASSERT_EQ(std::string(100, 'x'), val);
  }
}

INSTANTIATE_TEST_CASE_P(DBWriteTestInstance, DBWriteTest,
                        testing::Values(DBTestBase::kDefault,
                                        DBTestBase::kConcurrentWALWrites,
                                        DBTestBase::kPipelinedWrite));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
