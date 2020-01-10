//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/compaction_iterator.h"

#include <string>
#include <vector>

#include <iostream>
#include <thread>
#include <utility>
#include <iostream>
#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "db/change_stream_impl.h"
#include "db/version_set.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "util/fault_injection_test_env.h"
#include "util/sync_point.h"
#include "util/testharness.h"

namespace rocksdb {

class ChangeStreamTest : public testing::Test {
 public:
  DB* db;
  std::string dbname;
  Options options;

  ChangeStreamTest() {
    options.create_if_missing = true;
    dbname = "/tmp/change_stream_test";
    options.oldest_wal_seq_number = 1;
    options.WAL_size_limit_MB = 100;
    DestroyDB(dbname, options);
    Open();
  }
  ~ChangeStreamTest() {
    if (db) {
      delete db;
      DestroyDB(dbname, options);
    }
  }

  void Reopen(bool destroy = false) {
    delete db;
    db = nullptr;
    if (destroy) {
      DestroyDB(dbname, options);
    }
    Open();
  }

  void DynamicProduceConsume(size_t cfNum) {
    options.level0_file_num_compaction_trigger = 1000;
    options.level0_slowdown_writes_trigger = 1000;
    Reopen(true);
    std::vector<ColumnFamilyHandle*> handlers;
    ChangeStream* streamproxy = nullptr;
    std::string manifest;
    uint64_t lsn = 0;
    for (size_t i = 0; i < cfNum; ++i) {
      ColumnFamilyHandle* handler;
      ASSERT_OK(db->CreateColumnFamily(ColumnFamilyOptions(), std::to_string(i),
                                       &handler));
      handlers.push_back(handler);
    }
    ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                     &streamproxy, &manifest, &lsn));
    ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
    stream->onDurableLSN(kMaxSequenceNumber);

    std::atomic<bool> stop(false);
    std::vector<port::Thread> meta_producers(3);
    std::vector<port::Thread> data_producers(10);
    for (auto& t : data_producers) {
      t = port::Thread([&] {
        Random rnd(301);
        while (!stop) {
          auto cf = handlers[rnd.Next() % handlers.size()];
          db->Put(WriteOptions(), cf, DBTestBase::RandomString(&rnd, 10),
                  DBTestBase::RandomString(&rnd, 1000));
        }
      });
    }
    for (auto& t : meta_producers) {
      t = port::Thread([&] {
        Random rnd(301);
        while (!stop) {
          // auto cf = handlers[rnd.Next() % handlers.size()];
          // ASSERT_OK(db->Flush(FlushOptions(), cf));
          db->GetEnv()->SleepForMicroseconds(1000000);
        }
      });
    }
    ChangeEvent* event;
    uint64_t curr_lsn = 0;
    uint64_t count = 0;
    while (count < 1000000) {
      auto s = stream->NextEvent(&event);
      if (!s.ok()) {
        ASSERT_TRUE(s.IsTryAgain());
        db->GetEnv()->SleepForMicroseconds(10000);
        continue;
      }
      if (event->GetEventType() == CEType::CEWAL) {
        ASSERT_TRUE(event->GetWalEvent().sequence > curr_lsn);
        size_t batch_count =
            WriteBatchInternal::Count(event->GetWalEvent().writeBatchPtr.get());
        curr_lsn = event->GetWalEvent().sequence + batch_count - 1;
      } else {
        std::string ve_bin = event->GetMetaEvent()[0][0];
        VersionEdit ve;
        ASSERT_OK(ve.DecodeFrom(Slice(ve_bin.c_str(), ve_bin.size())));
        ASSERT_TRUE(ve.last_seq() == curr_lsn);
      }
      delete (event);
      count++;
      if (count % 10000 == 0) {
        std::cout << "current count:" << count << std::endl;
      }
    }
    stop = true;
    for (auto& t : meta_producers) {
      t.join();
    }
    for (auto& t : data_producers) {
      t.join();
    }
    ASSERT_OK(db->ReleaseChangeStream(streamproxy));
    for (auto& h : handlers) {
      ASSERT_OK(db->DestroyColumnFamilyHandle(h));
    }
  }

 private:
  void Open() {
    Status s = DB::Open(options, dbname, &db);
    assert(s.ok());
  }
};

TEST_F(ChangeStreamTest, DestroyCFBeforeReleaseStream) {
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  Reopen(true);
  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn = 0;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ASSERT_NE(manifest.size(), 0);
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  Random rnd(301);
  db->Put(WriteOptions(), DBTestBase::RandomString(&rnd, 10),
          DBTestBase::RandomString(&rnd, 10));

  ColumnFamilyHandle* handle_test1 = nullptr;

  ASSERT_OK(
      db->CreateColumnFamily(ColumnFamilyOptions(), "test1", &handle_test1));
  std::cout << "before drop\n";
  ASSERT_OK(db->DropColumnFamily(handle_test1));
  std::cout << "after drop\n";
  ASSERT_OK(db->DestroyColumnFamilyHandle(handle_test1));
  std::cout << "after dstroy\n";
  std::cout << streamproxy->DebugJSON(0);
  ASSERT_OK(db->ReleaseChangeStream(stream));
}

TEST_F(ChangeStreamTest, FlushCFAfterDropCF) {
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  Reopen(true);
  ColumnFamilyHandle* handle_test1 = nullptr;
  ASSERT_OK(
      db->CreateColumnFamily(ColumnFamilyOptions(), "test1", &handle_test1));

  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn = 0;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ASSERT_NE(manifest.size(), 0);
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);
  ASSERT_OK(db->DropColumnFamily(handle_test1));

  ChangeStream* streamproxy1 = nullptr;
  std::string manifest1;
  uint64_t lsn1 = 0;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy1, &manifest1, &lsn1));
  ASSERT_NE(manifest1.size(), 0);
  ChangeStreamImpl* stream1 = reinterpret_cast<ChangeStreamImpl*>(streamproxy1);
  stream1->onDurableLSN(kMaxSequenceNumber);

  ASSERT_OK(db->ReleaseChangeStream(stream));
  ASSERT_OK(db->ReleaseChangeStream(stream1));
  ASSERT_OK(db->DestroyColumnFamilyHandle(handle_test1));
}

TEST_F(ChangeStreamTest, StaticProduceConsume) {
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  Reopen(true);
  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn = 0;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ASSERT_NE(manifest.size(), 0);
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  Random rnd(301);
  for (size_t i = 0; i < 10; ++i) {
    db->Put(WriteOptions(), DBTestBase::RandomString(&rnd, 10),
            DBTestBase::RandomString(&rnd, 10));
    db->FlushWAL(true);
  }
  db->Flush(FlushOptions());
  for (size_t i = 0; i < 10; ++i) {
    db->Put(WriteOptions(), DBTestBase::RandomString(&rnd, 10),
            DBTestBase::RandomString(&rnd, 10));
    db->Flush(FlushOptions());
  }
  std::vector<std::unique_ptr<ChangeEvent>> events;
  ASSERT_EQ(stream->GetUnpublishedEventSize(), 1 + 1 + 10*2);
  ChangeEvent* event;
  while (true) {
    auto s = stream->NextEvent(&event);
    if (!s.ok()) {
      ASSERT_TRUE(s.IsTryAgain());
      break;
    }
    events.emplace_back(std::unique_ptr<ChangeEvent>(event));
  }
  ASSERT_EQ(events.size(), 10 + 1 + 10*2);
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
}

TEST_F(ChangeStreamTest, DynamicProduceConsume3) { DynamicProduceConsume(3); }

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


