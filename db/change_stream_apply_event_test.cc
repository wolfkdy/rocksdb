//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/compaction_iterator.h"

#include <string>
#include <vector>

#include <iostream>
#include <iostream>
#include <thread>
#include <utility>
#include "db/change_stream_impl.h"
#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "db/version_set.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "util/fault_injection_test_env.h"
#include "util/sync_point.h"
#include "util/testharness.h"

namespace rocksdb {

EventCFOptionFn dftCFOpt = [](const std::string& cfname) {
  return ColumnFamilyOptions();
};

class ApplyEventTest : public testing::Test {
 public:
  DB* db = nullptr;
  DB* readOnlydb = nullptr;
  DB* readOnlydb2 = nullptr;
  std::string dbname;
  Options options;
  Options options_read_replica;

  ApplyEventTest() {
    dbname = "/tmp/change_stream_apply_test";
    options.create_if_missing = true;
    options.oldest_wal_seq_number = 1;
    options.WAL_size_limit_MB = 200;
    DestroyDB(dbname, options);
    Open();
    options_read_replica.create_if_missing = true;
    options_read_replica.open_read_replica = true;
    options_read_replica.WAL_size_limit_MB = 200;
    OpenRealOnly();
  }

  ~ApplyEventTest() {
    if (db) {
      delete db;
      db = nullptr;
    }
    if (readOnlydb) {
      delete readOnlydb;
      readOnlydb = nullptr;
    }
    DestroyDB(dbname, options);
  }

  void Reopen(bool destroy = false) {
    delete db;
    db = nullptr;
    if (destroy) {
      ASSERT_TRUE(readOnlydb == nullptr);
      DestroyDB(dbname, options);
    }
    Open();
  }

  std::string Key(int i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "key%06d", i);
    return std::string(buf);
  }

  static std::string RandomString(Random* rnd, int len) {
    std::string r;
    test::RandomString(rnd, len, &r);
    return r;
  }

  int NumTableFilesAtLevel(int level, int cf, DB* destDb) {
    std::string property;
    assert(cf == 0);
    EXPECT_TRUE(destDb->GetProperty(
        "rocksdb.num-files-at-level" + NumberToString(level), &property));
    return atoi(property.c_str());
  }

  void CloseReadOnly() {
    assert(readOnlydb);
    delete readOnlydb;
    readOnlydb = nullptr;
  }

  void ReopenReadOnly() {
    delete readOnlydb;
    readOnlydb = nullptr;
    OpenRealOnly();
  }

  void ReopenReadOnly2() {
    delete readOnlydb2;
    readOnlydb2 = nullptr;
    OpenRealOnly2();
  }

  void Open() {
    Status s = DB::Open(options, dbname, &db);
    ASSERT_OK(s) << s.ToString();
  }
  void OpenRealOnly() {
    Status s = DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb);
    ASSERT_OK(s) << s.ToString();
  }

  void OpenRealOnly2() {
    Status s = DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb2);
    ASSERT_OK(s) << s.ToString();
  }
  DBImpl* dbfull() { return reinterpret_cast<DBImpl*>(db); }
};

TEST_F(ApplyEventTest, StaticManifest) {
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  CloseReadOnly();
  Reopen(true);
  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn = 0;
  std::vector<std::string> cfs;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));

  auto s = DB::ListColumnFamiliesInManifest(options_read_replica, dbname,
                                            manifest, &cfs);
  ASSERT_OK(s) << s.ToString();
  for (auto& v : cfs) {
    std::cout << "cfname:" << v << std::endl;
  }
  options_read_replica.read_replica_manifest = manifest;
  options_read_replica.read_replica_start_lsn = lsn;
  s = DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb, false);
  ASSERT_OK(s) << s.ToString();
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
}

TEST_F(ApplyEventTest, ConsistentLSN) {
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  CloseReadOnly();
  Reopen(true);

  for (size_t i = 0; i < 100000; ++i) {
    db->Put(WriteOptions(), std::string("a") + std::to_string(i),
            std::string("a_value"));
    if (i % 10000 == 0) {
      db->Flush(FlushOptions());
    }
  }
  // db->Flush(FlushOptions());

  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));

  options_read_replica.read_replica_manifest = manifest;
  options_read_replica.read_replica_start_lsn = lsn;
  auto s =
      DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb, false);
  ASSERT_EQ(readOnlydb->GetLatestSequenceNumber(),
            db->GetLatestSequenceNumber());
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
}

TEST_F(ApplyEventTest, DropZeroRef) {
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  CloseReadOnly();
  Reopen(true);

  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  options_read_replica.read_replica_manifest = manifest;
  options_read_replica.read_replica_start_lsn = lsn;
  auto s =
      DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb, false);

  ColumnFamilyHandle* handle_test1 = nullptr;
  ASSERT_OK(
      db->CreateColumnFamily(ColumnFamilyOptions(), "test1", &handle_test1));
  ASSERT_OK(db->DropColumnFamily(handle_test1));
  std::vector<std::unique_ptr<ChangeEvent>> events;
  auto applyToTheEnd = [&] {
    while (true) {
      ChangeEvent* event;
      s = streamproxy->NextEvent(&event);
      if (!s.ok()) {
        ASSERT_TRUE(s.IsTryAgain());
        break;
      }
      s = readOnlydb->ApplyEvent(*event, dftCFOpt);
      ASSERT_OK(s) << s.ToString();
      events.emplace_back(std::unique_ptr<ChangeEvent>(event));
    }
  };
  applyToTheEnd();
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
  ASSERT_OK(db->DestroyColumnFamilyHandle(handle_test1));
}

TEST_F(ApplyEventTest, StaticResync0) {
  // test that data residents in seconary's memtables should be clear after
  // resync
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  CloseReadOnly();
  Reopen(true);

  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  options_read_replica.read_replica_manifest = manifest;
  options_read_replica.read_replica_start_lsn = lsn;
  auto s =
      DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb, false);
  ASSERT_EQ(readOnlydb->GetLatestSequenceNumber(),
            db->GetLatestSequenceNumber());

  ASSERT_OK(db->Put(WriteOptions(), "hello_test1", "world"));
  std::vector<std::unique_ptr<ChangeEvent>> events;
  auto applyToTheEnd = [&] {
    while (true) {
      ChangeEvent* event;
      s = streamproxy->NextEvent(&event);
      if (!s.ok()) {
        ASSERT_TRUE(s.IsTryAgain());
        break;
      }
      s = readOnlydb->ApplyEvent(*event, dftCFOpt);
      ASSERT_OK(s) << s.ToString();
      events.emplace_back(std::unique_ptr<ChangeEvent>(event));
    }
  };
  applyToTheEnd();
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
  std::string result;
  ASSERT_OK(readOnlydb->Get(ReadOptions(), "hello_test1", &result));
  ASSERT_EQ(result, "world");
  ASSERT_OK(db->Put(WriteOptions(), "hello_test1", "world1"));
  ASSERT_OK(db->Flush(FlushOptions()));

  uint64_t snapshot_lsn = 0;
  s = db->DumpManifestSnapshot(&manifest, &snapshot_lsn);
  ASSERT_OK(s);
  ASSERT_EQ(snapshot_lsn, db->GetLatestSequenceNumber());
  std::vector<std::string> to_drop, to_create;
  s = readOnlydb->ReloadReadOnly(manifest, snapshot_lsn, dftCFOpt, &to_create,
                                 &to_drop);
  ASSERT_OK(s);
  ASSERT_EQ(readOnlydb->GetLatestSequenceNumber(), snapshot_lsn);

  // this assertion figures out an empty mem/imm
  ASSERT_OK(readOnlydb->Get(ReadOptions(), "hello_test1", &result));
  ASSERT_EQ(result, "world1");
}

TEST_F(ApplyEventTest, StaticResync1) {
  // test dynamic create/drop cf
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  CloseReadOnly();
  Reopen(true);

  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  options_read_replica.read_replica_manifest = manifest;
  options_read_replica.read_replica_start_lsn = lsn;
  auto s =
      DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb, false);
  ASSERT_EQ(readOnlydb->GetLatestSequenceNumber(),
            db->GetLatestSequenceNumber());

  ColumnFamilyHandle* handle_test1 = nullptr;
  ColumnFamilyHandle* handle_test2 = nullptr;
  ColumnFamilyHandle* handle_test1_read_only = nullptr;
  ColumnFamilyHandle* handle_test2_read_only = nullptr;
  ColumnFamilyHandle* handle_test3_read_only = nullptr;
  ASSERT_OK(
      db->CreateColumnFamily(ColumnFamilyOptions(), "test1", &handle_test1));
  ASSERT_OK(db->Put(WriteOptions(), handle_test1, "hello_test1", "world"));

  std::vector<std::unique_ptr<ChangeEvent>> events;
  auto applyToTheEnd = [&] {
    while (true) {
      ChangeEvent* event;
      s = stream->NextEvent(&event);
      if (!s.ok()) {
        ASSERT_TRUE(s.IsTryAgain());
        break;
      }
      s = readOnlydb->ApplyEvent(*event, dftCFOpt);
      ASSERT_OK(s) << s.ToString();
      events.emplace_back(std::unique_ptr<ChangeEvent>(event));
    }
  };
  applyToTheEnd();
  ASSERT_OK(
      readOnlydb->GetReadReplicaColumnFamily("test1", &handle_test1_read_only));
  std::string result;
  ASSERT_OK(readOnlydb->Get(ReadOptions(), handle_test1_read_only,
                            "hello_test1", &result));
  ASSERT_EQ(result, "world");
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
  ASSERT_OK(
      db->CreateColumnFamily(ColumnFamilyOptions(), "test2", &handle_test2));
  ASSERT_OK(db->Put(WriteOptions(), handle_test2, "hello_test2", "world"));

  uint64_t snapshot_lsn = 0;
  s = db->DumpManifestSnapshot(&manifest, &snapshot_lsn);
  ASSERT_OK(s);
  ASSERT_EQ(snapshot_lsn, db->GetLatestSequenceNumber());
  std::vector<std::string> to_drop, to_create;
  s = readOnlydb->ReloadReadOnly(manifest, snapshot_lsn, dftCFOpt, &to_create,
                                 &to_drop);
  ASSERT_OK(s);
  ASSERT_EQ(readOnlydb->GetLatestSequenceNumber(), snapshot_lsn);

  s = readOnlydb->GetReadReplicaColumnFamily("test2", &handle_test2_read_only);
  ASSERT_OK(s) << s.ToString();
  ASSERT_TRUE(handle_test2_read_only != nullptr);
  ASSERT_OK(readOnlydb->Get(ReadOptions(), handle_test2_read_only,
                            "hello_test2", &result));
  ASSERT_EQ(result, "world");

  ASSERT_OK(db->DropColumnFamily(handle_test1));
  ASSERT_OK(db->Put(WriteOptions(), handle_test2, "hello_test3", "world3"));
  ASSERT_OK(db->Flush(FlushOptions(), handle_test2));
  snapshot_lsn = 0;
  s = db->DumpManifestSnapshot(&manifest, &snapshot_lsn);
  ASSERT_OK(s);
  ASSERT_EQ(snapshot_lsn, db->GetLatestSequenceNumber());
  s = readOnlydb->ReloadReadOnly(manifest, snapshot_lsn, dftCFOpt, &to_create,
                                 &to_drop);
  ASSERT_OK(s);
  ASSERT_NOK(
      readOnlydb->GetReadReplicaColumnFamily("test1", &handle_test3_read_only));
  ASSERT_OK(readOnlydb->Get(ReadOptions(), handle_test2_read_only,
                            "hello_test3", &result));
  ASSERT_EQ(result, "world3");

  ASSERT_OK(db->DestroyColumnFamilyHandle(handle_test1));
  ASSERT_OK(db->DestroyColumnFamilyHandle(handle_test2));
  ASSERT_OK(readOnlydb->DestroyColumnFamilyHandle(handle_test1_read_only));
  ASSERT_OK(readOnlydb->DestroyColumnFamilyHandle(handle_test2_read_only));
}

TEST_F(ApplyEventTest, StaticProduceConsume) {
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  CloseReadOnly();
  Reopen(true);
  ReopenReadOnly();
  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  Random rnd(301);
  for (size_t i = 0; i < 10; ++i) {
    db->Put(WriteOptions(), std::string("aaa_") + std::to_string(i),
            std::string("aaa_value_") + std::to_string(i));
    db->FlushWAL(true);
  }
  db->Flush(FlushOptions());
  for (size_t i = 0; i < 10; ++i) {
    db->Put(WriteOptions(), "bbb_" + std::to_string(i),
            "bbb_value_" + std::to_string(i));
    db->Flush(FlushOptions());
  }
  std::vector<std::unique_ptr<ChangeEvent>> events;
  ASSERT_EQ(stream->GetUnpublishedEventSize(), 1 + 1 + 10 * 2);

  ChangeEvent* event;
  while (true) {
    auto s = stream->NextEvent(&event);
    if (!s.ok()) {
      ASSERT_TRUE(s.IsTryAgain());
      break;
    }
    s = readOnlydb->ApplyEvent(*event, dftCFOpt);
    ASSERT_OK(s) << s.ToString();
    events.emplace_back(std::unique_ptr<ChangeEvent>(event));
  }
  for (size_t i = 0; i < 10; ++i) {
    std::string result;
    Status s =
        readOnlydb->Get(ReadOptions(), ("aaa_" + std::to_string(i)), &result);
    ASSERT_OK(s);
    ASSERT_EQ("aaa_value_" + std::to_string(i), result);
  }

  for (size_t j = 0; j < 10; ++j) {
    std::string result;
    Status s =
        readOnlydb->Get(ReadOptions(), ("bbb_" + std::to_string(j)), &result);
    ASSERT_OK(s);
    ASSERT_EQ("bbb_value_" + std::to_string(j), result);
  }
  ASSERT_EQ(events.size(), 10 + 1 + 10 * 2);
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
}

TEST_F(ApplyEventTest, StaticProduceConsumeWithCreateDropCF) {
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  CloseReadOnly();
  Reopen(true);
  ReopenReadOnly();
  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  ColumnFamilyHandle* handle_test1 = nullptr;
  ColumnFamilyHandle* handle_test2 = nullptr;
  ASSERT_OK(
      db->CreateColumnFamily(ColumnFamilyOptions(), "test1", &handle_test1));
  ASSERT_OK(
      db->CreateColumnFamily(ColumnFamilyOptions(), "test2", &handle_test2));
  ASSERT_OK(db->Put(WriteOptions(), handle_test1, "hello_test1", "world"));
  ASSERT_OK(db->Put(WriteOptions(), handle_test2, "hello_test2", "world"));

  std::vector<std::unique_ptr<ChangeEvent>> events;
  auto applyToTheEnd = [&] {
    while (true) {
      ChangeEvent* event;
      auto s = stream->NextEvent(&event);
      if (!s.ok()) {
        ASSERT_TRUE(s.IsTryAgain());
        break;
      }
      s = readOnlydb->ApplyEvent(*event, dftCFOpt);
      ASSERT_OK(s) << s.ToString();
      events.emplace_back(std::unique_ptr<ChangeEvent>(event));
    }
  };
  applyToTheEnd();
  ColumnFamilyHandle* handle_test1_read_only = nullptr;
  ColumnFamilyHandle* handle_test2_read_only = nullptr;
  ASSERT_OK(
      readOnlydb->GetReadReplicaColumnFamily("test1", &handle_test1_read_only));
  ASSERT_OK(
      readOnlydb->GetReadReplicaColumnFamily("test2", &handle_test2_read_only));
  std::string result;
  ASSERT_OK(readOnlydb->Get(ReadOptions(), handle_test1_read_only,
                            "hello_test1", &result));
  ASSERT_EQ(result, "world");
  ASSERT_OK(readOnlydb->Get(ReadOptions(), handle_test2_read_only,
                            "hello_test2", &result));
  ASSERT_EQ(result, "world");
  ASSERT_OK(db->DropColumnFamily(handle_test1));
  applyToTheEnd();
  ColumnFamilyHandle* handle_test2_read_only_tmp = nullptr;
  ASSERT_TRUE(
      readOnlydb
          ->GetReadReplicaColumnFamily("test1", &handle_test2_read_only_tmp)
          .IsNotFound());
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
  ASSERT_OK(db->DestroyColumnFamilyHandle(handle_test1));
  ASSERT_OK(db->DestroyColumnFamilyHandle(handle_test2));
  ASSERT_OK(readOnlydb->DestroyColumnFamilyHandle(handle_test1_read_only));
  ASSERT_OK(readOnlydb->DestroyColumnFamilyHandle(handle_test2_read_only));
}

TEST_F(ApplyEventTest, DynamicProduceConsume) {
  options.level0_file_num_compaction_trigger = 1000;
  options.level0_slowdown_writes_trigger = 1000;
  CloseReadOnly();
  Reopen(true);
  ReopenReadOnly();
  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn;
  db->DisableFileDeletions();
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  std::atomic<bool> stop(false);
  std::vector<port::Thread> meta_producers(2);
  std::vector<port::Thread> data_producers(6);
  std::vector<port::Thread> background_reads(2);
  std::vector<port::Thread> background_scans(2);

  std::mutex keys_lk;
  std::set<std::string> keys;
  for (auto& t : data_producers) {
    t = port::Thread([&] {
      Random rnd(301);
      while (!stop) {
        std::string key = DBTestBase::RandomString(&rnd, 10);
        db->Put(WriteOptions(), key, DBTestBase::RandomString(&rnd, 1000));
        std::lock_guard<std::mutex> gd(keys_lk);
        keys.insert(key);
      }
    });
  }
  for (auto& t : meta_producers) {
    t = port::Thread([&] {
      while (!stop) {
        ASSERT_OK(db->Flush(FlushOptions()));
        db->GetEnv()->SleepForMicroseconds(1000000);
      }
    });
  }
  for (auto& t : background_reads) {
    t = port::Thread([&] {
      std::string tmp;
      Random rnd(301);
      while (!stop) {
        std::string key = DBTestBase::RandomString(&rnd, 10);
        auto s = readOnlydb->Get(ReadOptions(), key, &tmp);
        ASSERT_TRUE(s.ok() || s.IsNotFound());
      }
    });
  }
  for (auto& t : background_scans) {
    t = port::Thread([&] {
      std::string tmp;
      Random rnd(301);
      while (!stop) {
        std::string key = DBTestBase::RandomString(&rnd, 10);
        auto it =
            std::unique_ptr<Iterator>(readOnlydb->NewIterator(ReadOptions()));
        it->Seek(key);
        for (int i = 0; i < 100 && it->Valid(); it->Next())
          ;
        if (!it->Valid()) {
          ASSERT_TRUE(it->status().ok());
        }
      }
    });
  }
  ChangeEvent* event;
  uint64_t count = 0;
  while (count < 1000000) {
    auto s = stream->NextEvent(&event);
    if (!s.ok()) {
      ASSERT_TRUE(s.IsTryAgain());
      db->GetEnv()->SleepForMicroseconds(10000);
      continue;
    }
    ASSERT_OK(readOnlydb->ApplyEvent(*event, dftCFOpt));
    delete (event);
    count++;
    if (count % 1000 == 0) {
      std::cout << "replicate count:" << count << std::endl;
    }
  }
  stop = true;
  for (auto& t : meta_producers) {
    t.join();
  }
  for (auto& t : data_producers) {
    t.join();
  }
  for (auto& t : background_reads) {
    t.join();
  }
  for (auto& t : background_scans) {
    t.join();
  }

  // at last, we check both side
  auto it_left = std::unique_ptr<Iterator>(db->NewIterator(ReadOptions()));
  auto it_right =
      std::unique_ptr<Iterator>(readOnlydb->NewIterator(ReadOptions()));
  while (it_left->Valid() && it_right->Valid()) {
    ASSERT_EQ(it_left->key(), it_right->key());
    ASSERT_EQ(it_left->value(), it_right->value());
    it_left->Next();
    it_right->Next();
  }
  ASSERT_TRUE((!it_left->Valid()) && it_left->status().ok());
  ASSERT_TRUE((!it_right->Valid()) && it_right->status().ok());
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
}

// one replica, apply all ,then check
TEST_F(ApplyEventTest, HeartBeatForSameData) {
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  CloseReadOnly();
  Reopen(true);
  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  std::cout << "a stream create: " << streamproxy->DebugJSON(0) << std::endl;
  options_read_replica.read_replica_manifest = manifest;
  options_read_replica.read_replica_start_lsn = lsn;
  auto s =
      DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb, false);

  for (size_t i = 0; i < 100; ++i) {
    db->Put(WriteOptions(), std::string("a") + std::to_string(i),
            std::string("a_value"));
    if (i % 10 == 0) {
      db->Flush(FlushOptions());
      // std::cout << "after flush time " << i << streamproxy->DebugJSON(0) <<
      // std::endl;
    }
  }

  ChangeEvent* event;
  while (true) {
    s = streamproxy->NextEvent(&event);
    if (!s.ok()) {
      ASSERT_TRUE(s.IsTryAgain());
      break;
    }
    s = readOnlydb->ApplyEvent(*event, dftCFOpt);
    ASSERT_OK(s);
  }

  std::map<std::uint32_t, std::vector<uint64_t>> hints;

  s = readOnlydb->GetAllVersionsCreateHint(&hints);
  ASSERT_OK(s);
  std::map<std::uint32_t, std::set<uint64_t>> hb;
  for (auto& cfVersions : hints) {
    std::cout << "cfId:" << cfVersions.first << std::endl;
    hb[cfVersions.first] = {};
    for (auto& versionLsn : cfVersions.second) {
      std::cout << "version lsn:" << versionLsn << std::endl;
      hb[cfVersions.first].insert(versionLsn);
    }
    std::cout << "\n" << std::endl;
  }

  uint64_t current_readonly_lsn = readOnlydb->GetLatestSequenceNumber();
  ASSERT_EQ(readOnlydb->GetLatestSequenceNumber(), current_readonly_lsn);
  std::map<uint32_t, std::list<uint64_t>> allVersion;
  allVersion.clear();
  std::cout << "before heartbeat primairy versions: "
            << streamproxy->DebugJSON(0) << std::endl;
  streamproxy->GetInUsedVersonForTest(allVersion);
  ASSERT_EQ(11, allVersion[0].size());
  s = streamproxy->HeartBeat(current_readonly_lsn, hb);
  ASSERT_OK(s);
  std::cout << "after heartbeat primairy versions: "
            << streamproxy->DebugJSON(0) << std::endl;
  streamproxy->GetInUsedVersonForTest(allVersion);
  ASSERT_EQ(1, allVersion[0].size());
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
}

// one replica, apply some ,then check, apply other than check , apply left ,
// then check

TEST_F(ApplyEventTest, HeartBeatForSameDataStepbyStep) {
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  CloseReadOnly();
  Reopen(true);
  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  std::cout << "a stream create: " << streamproxy->DebugJSON(0) << std::endl;

  options_read_replica.read_replica_manifest = manifest;
  options_read_replica.read_replica_start_lsn = lsn;
  auto s =
      DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb, false);
  for (size_t i = 0; i < 100; ++i) {
    db->Put(WriteOptions(), std::string("a") + std::to_string(i),
            std::string("a_value"));
    if (i % 10 == 0) {
      db->Flush(FlushOptions());
      // std::cout << "after flush time " << i << streamproxy->DebugJSON(0) <<
      // std::endl;
    }
  }
  ChangeEvent* event;
  uint64_t current_readonly_lsn = readOnlydb->GetLatestSequenceNumber();
  std::cout << "before apply  ,readonly lastes seq:" << current_readonly_lsn
            << "\n"
            << std::endl;
  for (size_t i = 0; i < 3; ++i) {
    s = streamproxy->NextEvent(&event);
    if (!s.ok()) {
      ASSERT_TRUE(s.IsTryAgain());
      break;
    }
    s = readOnlydb->ApplyEvent(*event, dftCFOpt);
    ASSERT_OK(s);
  }

  std::map<std::uint32_t, std::vector<uint64_t>> hints;

  s = readOnlydb->GetAllVersionsCreateHint(&hints);
  ASSERT_OK(s);
  std::map<std::uint32_t, std::set<uint64_t>> hb;
  for (auto& cfVersions : hints) {
    std::cout << "cfId:" << cfVersions.first << std::endl;
    hb[cfVersions.first] = {};
    for (auto& versionLsn : cfVersions.second) {
      std::cout << "version lsn:" << versionLsn << std::endl;
      hb[cfVersions.first].insert(versionLsn);
    }
    std::cout << "\n" << std::endl;
  }
  current_readonly_lsn = readOnlydb->GetLatestSequenceNumber();
  std::cout << "after 3 ,readonly lastes seq " << current_readonly_lsn << "\n"
            << std::endl;
  std::map<uint32_t, std::list<uint64_t>> allVersion;
  allVersion.clear();
  std::cout << "before heartbeat primairy versions: "
            << streamproxy->DebugJSON(0) << std::endl;
  streamproxy->GetInUsedVersonForTest(allVersion);
  ASSERT_EQ(2, allVersion[0].size());
  s = streamproxy->HeartBeat(current_readonly_lsn, hb);
  ASSERT_OK(s);
  std::cout << "after heartbeat primairy versions: "
            << streamproxy->DebugJSON(0) << std::endl;
  streamproxy->GetInUsedVersonForTest(allVersion);
  ASSERT_EQ(1, allVersion[0].size());
  std::cout << "after heartbeat 3 ,primary version size: "
            << allVersion[0].size() << "\n"
            << std::endl;

  // last apply to 2, this apply to
  for (size_t i = 0; i < 30; ++i) {
    s = streamproxy->NextEvent(&event);
    if (!s.ok()) {
      ASSERT_TRUE(s.IsTryAgain());
      break;
    }
    s = readOnlydb->ApplyEvent(*event, dftCFOpt);
    ASSERT_OK(s);
  }
  streamproxy->GetInUsedVersonForTest(allVersion);
  ASSERT_EQ(3, allVersion[0].size());
  s = streamproxy->HeartBeat(current_readonly_lsn,
                             hb);  // do not update lsn, use last hb
  ASSERT_OK(s);

  current_readonly_lsn = readOnlydb->GetLatestSequenceNumber();
  std::cout << "after applay 33 ,readonly lastes seq " << current_readonly_lsn
            << "\n"
            << std::endl;
  ASSERT_EQ(30, current_readonly_lsn);
  std::cout << "after heartbeat, primairy versions: "
            << streamproxy->DebugJSON(0) << std::endl;
  streamproxy->GetInUsedVersonForTest(allVersion);
  ASSERT_EQ(3, allVersion[0].size());

  std::map<std::uint32_t, std::vector<uint64_t>> hints2;
  std::map<std::uint32_t, std::set<uint64_t>> hb2;
  s = readOnlydb->GetAllVersionsCreateHint(&hints2);
  ASSERT_OK(s);
  for (auto& cfVersions : hints2) {
    std::cout << "cfId:" << cfVersions.first << std::endl;
    hb2[cfVersions.first] = {};
    for (auto& versionLsn : cfVersions.second) {
      std::cout << "version lsn:" << versionLsn << std::endl;
      hb2[cfVersions.first].insert(versionLsn);
    }
    std::cout << "\n" << std::endl;
  }
  s = streamproxy->HeartBeat(current_readonly_lsn, hb2);
  ASSERT_OK(s);
  streamproxy->GetInUsedVersonForTest(allVersion);
  ASSERT_EQ(1, allVersion[0].size());
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
}

// two replica, apply all, then check

TEST_F(ApplyEventTest, HeartBeatTwoReplica) {
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 100;
  CloseReadOnly();
  Reopen(true);
  ChangeStream* streamproxy = nullptr;
  ChangeStream* streamproxy2 = nullptr;
  std::string manifest;
  std::string manifest2;
  uint64_t lsn;
  uint64_t lsn2;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  std::cout << "a stream create: " << streamproxy->DebugJSON(0) << std::endl;

  options_read_replica.read_replica_manifest = manifest;
  options_read_replica.read_replica_start_lsn = lsn;
  auto s =
      DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb, false);

  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy2, &manifest, &lsn2));
  ChangeStreamImpl* stream2 = reinterpret_cast<ChangeStreamImpl*>(streamproxy2);
  stream2->onDurableLSN(kMaxSequenceNumber);

  std::cout << "a stream 2 create: " << streamproxy2->DebugJSON(0) << std::endl;

  options_read_replica.read_replica_manifest = manifest2;
  options_read_replica.read_replica_start_lsn = lsn2;
  s = DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb2, false);

  for (size_t i = 0; i < 100; ++i) {
    db->Put(WriteOptions(), std::string("a") + std::to_string(i),
            std::string("a_value"));
    if (i % 10 == 0) {
      db->Flush(FlushOptions());
      std::cout << "after flush time " << i << streamproxy->DebugJSON(0)
                << std::endl;
    }
  }

  ChangeEvent* event;
  while (true) {
    s = streamproxy->NextEvent(&event);
    if (!s.ok()) {
      ASSERT_TRUE(s.IsTryAgain());
      break;
    }
    s = readOnlydb->ApplyEvent(*event, dftCFOpt);
    ASSERT_OK(s);
  }
  std::map<std::uint32_t, std::vector<uint64_t>> hints;

  s = readOnlydb->GetAllVersionsCreateHint(&hints);
  ASSERT_OK(s);
  std::map<std::uint32_t, std::set<uint64_t>> hb;
  for (auto& cfVersions : hints) {
    std::cout << "cfId:" << cfVersions.first << std::endl;
    hb[cfVersions.first] = {};
    for (auto& versionLsn : cfVersions.second) {
      std::cout << "version lsn:" << versionLsn << std::endl;
      hb[cfVersions.first].insert(versionLsn);
    }
    std::cout << "\n" << std::endl;
  }

  uint64_t current_readonly_lsn = readOnlydb->GetLatestSequenceNumber();
  ASSERT_EQ(readOnlydb->GetLatestSequenceNumber(), current_readonly_lsn);
  std::map<uint32_t, std::list<uint64_t>> allVersion;
  allVersion.clear();
  std::cout << "before heartbeat primairy versions: "
            << streamproxy->DebugJSON(0) << std::endl;
  streamproxy->GetInUsedVersonForTest(allVersion);
  ASSERT_EQ(11, allVersion[0].size());
  s = streamproxy->HeartBeat(current_readonly_lsn, hb);
  ASSERT_OK(s);
  std::cout << "after heartbeat primairy versions: "
            << streamproxy->DebugJSON(0) << std::endl;
  streamproxy->GetInUsedVersonForTest(allVersion);
  // TODO check versonset , all version still live
  ASSERT_EQ(1, allVersion[0].size());

  // check for read replicat 2
  streamproxy2->GetInUsedVersonForTest(allVersion);
  ASSERT_EQ(1, allVersion[0].size());
  while (true) {
    s = streamproxy2->NextEvent(&event);
    if (!s.ok()) {
      ASSERT_TRUE(s.IsTryAgain());
      break;
    }
    s = readOnlydb2->ApplyEvent(*event, dftCFOpt);
    ASSERT_OK(s);
  }
  allVersion.clear();
  streamproxy2->GetInUsedVersonForTest(allVersion);
  ASSERT_EQ(11, allVersion[0].size());

  current_readonly_lsn = readOnlydb2->GetLatestSequenceNumber();
  hb.clear();
  hints.clear();
  s = readOnlydb2->GetAllVersionsCreateHint(&hints);
  for (auto& cfVersions : hints) {
    std::cout << "cfId:" << cfVersions.first << std::endl;
    hb[cfVersions.first] = {};
    for (auto& versionLsn : cfVersions.second) {
      std::cout << "version lsn:" << versionLsn << std::endl;
      hb[cfVersions.first].insert(versionLsn);
    }
    std::cout << "\n" << std::endl;
  }
  s = streamproxy2->HeartBeat(current_readonly_lsn, hb);
  ASSERT_OK(s);
  streamproxy2->GetInUsedVersonForTest(allVersion);
  ASSERT_EQ(1, allVersion[0].size());
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
  ASSERT_OK(db->ReleaseChangeStream(streamproxy2));
}

TEST_F(ApplyEventTest, TrivialMoveOneFileForReload) {
  int32_t trivial_move = 0;
  CloseReadOnly();
  Reopen(true);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  ChangeStream* streamproxy = nullptr;
  std::string manifest;
  uint64_t lsn;
  ASSERT_OK(db->CreateChangeStream(ChangeStream::ManifestType::MANIFEST_FILE,
                                   &streamproxy, &manifest, &lsn));
  ChangeStreamImpl* stream = reinterpret_cast<ChangeStreamImpl*>(streamproxy);
  stream->onDurableLSN(kMaxSequenceNumber);

  options_read_replica.read_replica_manifest = manifest;
  options_read_replica.read_replica_start_lsn = lsn;
  Status s =
      DB::OpenForReadOnly(options_read_replica, dbname, &readOnlydb, false);
  ASSERT_EQ(readOnlydb->GetLatestSequenceNumber(),
            db->GetLatestSequenceNumber());
  options.write_buffer_size = 100000000;
  options.max_subcompactions = 0;

  int32_t num_keys = 80;
  int32_t value_size = 100 * 1024;  // 100 KB

  Random rnd(301);
  std::vector<std::string> values;
  for (int i = 0; i < num_keys; i++) {
    values.push_back(RandomString(&rnd, value_size));
    ASSERT_OK(db->Put(WriteOptions(), Key(i), values[i]));
  }
  db->Flush(FlushOptions());
  ASSERT_EQ(NumTableFilesAtLevel(0, 0, db), 1);  // 1 file in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0, db), 0);  // 0 files in L1
  std::vector<std::unique_ptr<ChangeEvent>> events;
  auto applyToTheEnd = [&] {
    while (true) {
      ChangeEvent* event;
      s = stream->NextEvent(&event);
      if (!s.ok()) {
        ASSERT_TRUE(s.IsTryAgain());
        break;
      }
      s = readOnlydb->ApplyEvent(*event, dftCFOpt);
      ASSERT_OK(s) << s.ToString();
      events.emplace_back(std::unique_ptr<ChangeEvent>(event));
    }
  };
  applyToTheEnd();

  ASSERT_EQ(NumTableFilesAtLevel(0, 0, readOnlydb), 1);  // 0 files in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0, readOnlydb), 0);  // 1 file in L1

  std::vector<LiveFileMetaData> metadata;
  db->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1U);
  LiveFileMetaData level0_file = metadata[0];  // L0 file meta

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = true;

  // Compaction will initiate a trivial move from L0 to L1
  dbfull()->CompactRange(cro, nullptr, nullptr);

  // File moved From L0 to L1
  ASSERT_EQ(NumTableFilesAtLevel(0, 0, db), 0);  // 0 files in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0, db), 1);  // 1 file in L1

  metadata.clear();
  db->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1U);
  ASSERT_EQ(metadata[0].name /* level1_file.name */, level0_file.name);
  ASSERT_EQ(metadata[0].size /* level1_file.size */, level0_file.size);

  for (int i = 0; i < num_keys; i++) {
    std::string result;
    ASSERT_OK(db->Get(ReadOptions(), Key(i), &result));
    ASSERT_EQ(result, values[i]);
  }
  ASSERT_EQ(NumTableFilesAtLevel(0, 0, readOnlydb), 1);  // 0 files in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0, readOnlydb), 0);  // 0 file in L1

  manifest = "";
  uint64_t snapshot_lsn = 0;
  s = db->DumpManifestSnapshot(&manifest, &snapshot_lsn);
  ASSERT_OK(s);
  ASSERT_EQ(snapshot_lsn, db->GetLatestSequenceNumber());
  std::vector<std::string> to_drop, to_create;
  s = readOnlydb->ReloadReadOnly(manifest, snapshot_lsn, dftCFOpt, &to_create,
                                 &to_drop);
  ASSERT_EQ(snapshot_lsn, readOnlydb->GetLatestSequenceNumber());
  ASSERT_EQ(NumTableFilesAtLevel(0, 0, readOnlydb), 0);  // 0 files in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0, readOnlydb), 1);  // 1 file in L1
  for (int i = 0; i < num_keys; i++) {
    std::string result;
    ASSERT_OK(readOnlydb->Get(ReadOptions(), Key(i), &result));
    ASSERT_EQ(result, values[i]);
  }
  ASSERT_EQ(trivial_move, 1);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  CloseReadOnly();
  ASSERT_OK(db->ReleaseChangeStream(streamproxy));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
