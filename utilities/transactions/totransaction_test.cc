//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include <functional>
#include <string>
#include <thread>

#include "port/port.h"
#include "rocksdb/db.h"
#include "totransaction_db_impl.h"
#include "totransaction_impl.h"
#include "util/crc32c.h"
#include "util/logging.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/transaction_test_util.h"
#include "port/port.h"
#include "db/db_test_util.h"

using std::string;

namespace rocksdb {

class TOTransactionTest : public testing::Test {
 public:
  TOTransactionDB* txn_db;
  string dbname;
  Options options;
  TOTransactionDBOptions txndb_options;
  TOTransactionOptions txn_options;

  TOTransactionTest() {
    options.create_if_missing = true;
    options.max_write_buffer_number = 2;
    dbname = /*test::TmpDir() +*/ "./totransaction_testdb";

    DestroyDB(dbname, options);
    Open();
  }
  ~TOTransactionTest() {
    delete txn_db;
    DestroyDB(dbname, options);
  }

  void Reopen() {
    delete txn_db;
    txn_db = nullptr;
    Open();
  }

void Reopen(Options newOptions) {
  delete txn_db;
  txn_db = nullptr;
  Open(newOptions);
}

private:
  void Open() {
    Status s = TOTransactionDBImpl::Open(options, txndb_options, dbname, &txn_db);
    assert(s.ok());
    assert(txn_db != nullptr);
  }
    void Open(Options newOptions) {
      Status s = TOTransactionDBImpl::Open(newOptions, txndb_options, dbname, &txn_db);
      assert(s.ok());
      assert(txn_db != nullptr);
    }
};

TEST_F(TOTransactionTest, ValidateIOWithoutTimestamp) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;
  TOTransactionStat stat;

  ASSERT_OK(s);
  // txn1 test put and get
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;
  txn = txn_db->BeginTransaction(write_options, txn_options);

  txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");
  
  s = txn->Commit();
  ASSERT_OK(s);

  s = txn_db->Stat(&stat);
  ASSERT_OK(s);
  ASSERT_EQ(stat.commit_without_ts_times, 2);
  ASSERT_EQ(stat.read_without_ts_times, 2);
  ASSERT_EQ(stat.txn_commits, 2);
  ASSERT_EQ(stat.txn_aborts, 0);
  delete txn;
}

TEST_F(TOTransactionTest, ValidateIO) {
  WriteOptions write_options;
  ReadOptions read_options;
  read_options.read_timestamp = 50;
  string value;
  Status s;
  TOTransactionStat stat;

  s = txn_db->SetTimeStamp(kOldest, 10, false);
  ASSERT_OK(s);
  // txn1 test put and get
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  s = txn->SetReadTimeStamp(50);
  ASSERT_OK(s);

  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s); 
 
  s = txn->Put(Slice("key3"), Slice("value3"));
  ASSERT_OK(s); 

  s = txn->SetCommitTimeStamp(100);
  ASSERT_OK(s);
  
  // Read your write
  s = txn->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
  s = txn_db->Stat(&stat);
  ASSERT_OK(s);
  ASSERT_EQ(stat.commit_without_ts_times, 0);
  ASSERT_EQ(stat.read_without_ts_times, 0);
  ASSERT_EQ(stat.txn_commits, 1);
  ASSERT_EQ(stat.txn_aborts, 0);
  ASSERT_EQ(stat.read_q_walk_times, 0);
  ASSERT_EQ(stat.commit_q_walk_times, 0);

  //
  // txn2  test iterator
  s = txn_db->SetTimeStamp(kOldest, 10, false);

  txn = txn_db->BeginTransaction(write_options, txn_options);
  txn->SetReadTimeStamp(101);

  txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");
 
  s = txn->Put(Slice("foo"), Slice("bar2"));
  ASSERT_OK(s);

  s = txn->Put(Slice("key1"), Slice("value1"));
  ASSERT_OK(s);
  s = txn->Put(Slice("key2"), Slice("value2"));
  ASSERT_OK(s);
  
  s = txn->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  Iterator* iter = txn->GetIterator(read_options);
  ASSERT_TRUE(iter);

  iter->SeekToFirst();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());

  Slice key = iter->key();
  Slice val = iter->value();

  ASSERT_EQ(key.ToString(), "foo");
  ASSERT_EQ(val, "bar2") << val.ToString();

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "key1");
  ASSERT_EQ(val, "value1");

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "key2");
  ASSERT_EQ(val, "value2");

  delete iter;
  
  s = txn->SetCommitTimeStamp(105);
  ASSERT_OK(s);
  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
  s = txn_db->Stat(&stat);
  ASSERT_OK(s);
  ASSERT_EQ(stat.commit_without_ts_times, 0);
  ASSERT_EQ(stat.read_without_ts_times, 0);
  ASSERT_EQ(stat.txn_commits, 2);
  ASSERT_EQ(stat.txn_aborts, 0);
  ASSERT_EQ(stat.read_q_num, 1);
  ASSERT_EQ(stat.commit_q_num, 1);

  // txn3 test write conflict
  txn = txn_db->BeginTransaction(write_options, txn_options);
  s = txn->SetReadTimeStamp(101);
  ASSERT_OK(s);

  txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");
 
  s = txn->Put(Slice("key4"), Slice("value4"));
  ASSERT_OK(s);

  // Write Conflict here, there is a txn committed before
  // whose commit ts is greater than my read ts
  s = txn->Put(Slice("key1"), Slice("value1"));
  ASSERT_TRUE(s.IsBusy());

  s = txn->Rollback();
  ASSERT_OK(s);
  
  delete txn;
  s = txn_db->Stat(&stat);
  ASSERT_OK(s);
  ASSERT_EQ(stat.commit_without_ts_times, 0);
  ASSERT_EQ(stat.read_without_ts_times, 0);
  ASSERT_EQ(stat.txn_commits, 2);
  ASSERT_EQ(stat.txn_aborts, 1);
  ASSERT_EQ(stat.read_q_num, 1);
  ASSERT_EQ(stat.commit_q_num, 1);

  // txn4 
  txn = txn_db->BeginTransaction(write_options, txn_options);
  s = txn->SetReadTimeStamp(106);
  ASSERT_OK(s);

  // No write conflict here
  s = txn->Put(Slice("key1"), Slice("value1"));
  ASSERT_OK(s);
  
  s = txn->SetCommitTimeStamp(110);
  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;

  // txn5 test delete
  txn = txn_db->BeginTransaction(write_options, txn_options);
  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);

  s = txn2->SetReadTimeStamp(110);
  ASSERT_OK(s);
  s = txn->SetReadTimeStamp(110);
  ASSERT_OK(s);

  s = txn2->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  s = txn->Delete(Slice("foo"));
  ASSERT_OK(s);

  s = txn->SetCommitTimeStamp(120);
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);

  // snapshot isolation
  s = txn2->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  s = txn2->SetCommitTimeStamp(121);
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);

  s = txn_db->Stat(&stat);
  ASSERT_OK(s);
  ASSERT_EQ(stat.commit_without_ts_times, 0);
  ASSERT_EQ(stat.read_without_ts_times, 0);
  ASSERT_EQ(stat.txn_commits, 5);
  ASSERT_EQ(stat.txn_aborts, 1);

  delete txn;
  delete txn2;
  
}

TEST_F(TOTransactionTest, ValidateWriteConflict) {
  WriteOptions write_options;
  ReadOptions read_options;
  read_options.read_timestamp = 50;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 10, false);
  ASSERT_OK(s);
  // txn1 test write conflict
  // txn1 and txn2 both modify foo
  // first update wins
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  ASSERT_TRUE(txn->GetID() < txn2->GetID());

  s = txn->SetReadTimeStamp(50);
  ASSERT_OK(s);

  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s); 
 
  s = txn2->Put(Slice("foo"), Slice("bar2"));
  ASSERT_TRUE(s.IsBusy()); 

  s = txn2->Rollback();
  ASSERT_OK(s);

  delete txn2;

  s = txn->SetCommitTimeStamp(100);
  ASSERT_OK(s);
  
  // Read your write
  s = txn->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
  // txn2  test write conflict
  // txn1 began before txn2, txn2 modified foo and commit
  // txn1 tried to modify foo
  s = txn_db->SetTimeStamp(kOldest, 10, false);

  txn = txn_db->BeginTransaction(write_options, txn_options);
  txn->SetReadTimeStamp(101);
  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  txn2->SetReadTimeStamp(101);

  txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");
 
  s = txn2->Put(Slice("foo"), Slice("bar2"));
  ASSERT_OK(s);

  s = txn2->Put(Slice("key1"), Slice("value1"));
  ASSERT_OK(s);
  s = txn2->Put(Slice("key2"), Slice("value2"));
  ASSERT_OK(s);
  
  s = txn2->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  Iterator* iter = txn2->GetIterator(read_options);
  ASSERT_TRUE(iter);

  iter->SeekToFirst();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());

  Slice key = iter->key();
  Slice val = iter->value();

  ASSERT_EQ(key.ToString(), "foo");
  ASSERT_EQ(val, "bar2");

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "key1");
  ASSERT_EQ(val, "value1");

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "key2");
  ASSERT_EQ(val, "value2");

  delete iter;
  
  s = txn2->SetCommitTimeStamp(105);
  s = txn2->Commit();
  ASSERT_OK(s);

  s = txn->Put(Slice("foo"), Slice("bar3"));
  ASSERT_TRUE(s.IsBusy());

  s = txn->Rollback();
  ASSERT_OK(s);

  delete txn;
  delete txn2;
  // txn3 test write conflict
  txn = txn_db->BeginTransaction(write_options, txn_options);
  s = txn->SetReadTimeStamp(101);
  ASSERT_OK(s);

  txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");
 
  s = txn->Put(Slice("key4"), Slice("value4"));
  ASSERT_OK(s);

  // Write Conflict here, there is a txn committed before
  // whose commit ts is greater than my read ts
  s = txn->Put(Slice("key1"), Slice("value1_1"));
  ASSERT_TRUE(s.IsBusy());

  s = txn->Rollback();
  ASSERT_OK(s);
  
  delete txn;

  // txn4 
  txn = txn_db->BeginTransaction(write_options, txn_options);
  s = txn->SetReadTimeStamp(106);
  ASSERT_OK(s);

  // No write conflict here
  s = txn->Put(Slice("key1"), Slice("value1"));
  ASSERT_OK(s);
  
  s = txn->SetCommitTimeStamp(110);
  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
}

TEST_F(TOTransactionTest, ValidateIsolation) {
  WriteOptions write_options;
  ReadOptions read_options;
  read_options.read_timestamp = 50;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 10, false);
  ASSERT_OK(s);
  // txn1 test snapshot isolation
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  ASSERT_TRUE(txn->GetID() < txn2->GetID());

  s = txn->SetReadTimeStamp(50);
  ASSERT_OK(s);

  s = txn->Put(Slice("A"), Slice("A-A"));
  ASSERT_OK(s); 

  s = txn->SetCommitTimeStamp(100);
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);
  
  RocksTimeStamp all_committed_ts;
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);

  s = txn2->Put(Slice("B"), Slice("B-B"));
  ASSERT_OK(s); 

  s = txn2->SetCommitTimeStamp(110);
  ASSERT_OK(s);

  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);

  TOTransaction* txn4 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn4);

  s = txn3->SetReadTimeStamp(60);
  ASSERT_OK(s);

  s = txn4->SetReadTimeStamp(110);
  ASSERT_OK(s);

  s = txn3->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn4->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "A-A");
  
  s = txn3->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn4->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn2->Commit();
  ASSERT_OK(s);

  s = txn3->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn4->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn3->Rollback();
  ASSERT_OK(s);

  s = txn4->Rollback();
  ASSERT_OK(s);

  delete txn;
  delete txn2;
  delete txn3;
  delete txn4;

  txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  s = txn->SetReadTimeStamp(110);
  ASSERT_OK(s);

  s = txn2->SetReadTimeStamp(110);
  ASSERT_OK(s);

  s = txn->Put(Slice("C"), Slice("C-C"));
  ASSERT_OK(s);

  s = txn->Put(Slice("H"), Slice("H-H"));
  ASSERT_OK(s);

  s = txn->Put(Slice("J"), Slice("J-J"));
  ASSERT_OK(s);
  
  Iterator* iter = txn->GetIterator(read_options);
  ASSERT_TRUE(iter);

  iter->SeekToFirst();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());

  Slice key = iter->key();
  Slice val = iter->value();

  ASSERT_EQ(key.ToString(), "A");
  ASSERT_EQ(val, "A-A");

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "B");
  ASSERT_EQ(val, "B-B");

  s = txn2->Put(Slice("E"), Slice("E-E"));
  ASSERT_OK(s);

  s = txn2->SetCommitTimeStamp(120);
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "C");
  ASSERT_EQ(val, "C-C");

  s = txn->Put(Slice("D"), Slice("D-D"));
  ASSERT_OK(s);

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "D");
  ASSERT_EQ(val, "D-D");

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "H");
  ASSERT_EQ(val, "H-H");

  s = txn->Put(Slice("F"), Slice("F-F"));
  ASSERT_OK(s);

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "J");
  ASSERT_EQ(val, "J-J");
  
  delete iter;
  
  s = txn->SetCommitTimeStamp(120);
  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
  delete txn2;
}


TEST_F(TOTransactionTest, CommitTsCheck) {
  WriteOptions write_options;
  ReadOptions read_options;
  read_options.read_timestamp = 50;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 10, false);
  ASSERT_OK(s);
  
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  s = txn->SetReadTimeStamp(100);
  ASSERT_OK(s);
  
  s = txn->SetCommitTimeStamp(120);
  ASSERT_OK(s);

  s = txn2->SetReadTimeStamp(100);
  ASSERT_OK(s);
  
  s = txn2->SetCommitTimeStamp(130);
  ASSERT_OK(s);
  
  RocksTimeStamp all_committed_ts;
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_TRUE(s.IsNotFound());
  
  s = txn2->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(all_committed_ts, 119);

  s = txn->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 130);
  
  delete txn;
  delete txn2;
  
}

TEST_F(TOTransactionTest, CommitTsCheck2) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 10, false);
  ASSERT_OK(s);
  
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);

  s = txn->SetReadTimeStamp(100);
  ASSERT_OK(s);
  
  s = txn->SetCommitTimeStamp(100);
  ASSERT_OK(s);

  s = txn2->SetReadTimeStamp(100);
  ASSERT_OK(s);
  
  s = txn2->SetCommitTimeStamp(120);
  ASSERT_OK(s);

  s = txn3->SetReadTimeStamp(100);
  ASSERT_OK(s);
  
  s = txn3->SetCommitTimeStamp(130);
  ASSERT_OK(s);
  
  RocksTimeStamp all_committed_ts;
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Put(Slice("1"), Slice("1"));
  ASSERT_TRUE(s.ok());
  
  s = txn->Commit();
  ASSERT_TRUE(s.ok());
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 100);
  
  s = txn3->Put(Slice("3"), Slice("3"));
  ASSERT_TRUE(s.ok());
  
  s = txn3->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 119);
  
  s = txn2->Put(Slice("2"), Slice("2"));
  ASSERT_TRUE(s.ok());
  
  s = txn2->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 130);
  
  delete txn;
  delete txn2;
  delete txn3;
  
}

//no put
TEST_F(TOTransactionTest, CommitTsCheck3) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 10, false);
  ASSERT_OK(s);
  
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);

  s = txn->SetReadTimeStamp(100);
  ASSERT_OK(s);
  
  s = txn->SetCommitTimeStamp(100);
  ASSERT_OK(s);

  s = txn2->SetReadTimeStamp(100);
  ASSERT_OK(s);
  
  s = txn2->SetCommitTimeStamp(120);
  ASSERT_OK(s);

  s = txn3->SetReadTimeStamp(100);
  ASSERT_OK(s);
  
  s = txn3->SetCommitTimeStamp(130);
  ASSERT_OK(s);
  
  RocksTimeStamp all_committed_ts;
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_TRUE(s.IsNotFound());
  
  s = txn->Commit();
  ASSERT_TRUE(s.ok());
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 100);
  
  ASSERT_TRUE(s.ok());
  
  s = txn3->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 119);
  
  ASSERT_TRUE(s.ok());
  
  s = txn2->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 130);
  
  delete txn;
  delete txn2;
  delete txn3;
  
}

TEST_F(TOTransactionTest, CommitTsCheck4) {
  WriteOptions write_options;
  ReadOptions read_options;
  RocksTimeStamp all_committed_ts;
  string value;
  Status s;

  TOTransaction* txn_ori = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn_ori);
  s = txn_ori->SetCommitTimeStamp(4);
  ASSERT_OK(s);
  s = txn_ori->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 4);

  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);
 
  TOTransaction* txn4 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn4);

  s = txn->SetReadTimeStamp(all_committed_ts);
  ASSERT_OK(s);

  s = txn2->SetCommitTimeStamp(5);
  ASSERT_OK(s);
  
  s = txn3->SetCommitTimeStamp(6);
  ASSERT_OK(s);
  
  s = txn2->Commit();
  ASSERT_OK(s);
  s = txn3->Commit();
  ASSERT_OK(s);

  s = txn4->SetCommitTimeStamp(6);
  ASSERT_OK(s);
  s = txn4->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 6);

  delete txn_ori;  
  delete txn;
  delete txn2;
  delete txn3;
  delete txn4;
}

TEST_F(TOTransactionTest, Rollback) {
  WriteOptions write_options;
  ReadOptions read_options;
  RocksTimeStamp all_committed_ts;
  string value;
  Status s;

  TOTransaction* txn_ori = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn_ori);
  s = txn_ori->SetCommitTimeStamp(4);
  ASSERT_OK(s);
  s = txn_ori->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 4);

  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);
 
  TOTransaction* txn4 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn4);

  s = txn->SetReadTimeStamp(all_committed_ts);
  ASSERT_OK(s);

  s = txn->SetCommitTimeStamp(5);
  ASSERT_OK(s);

  s = txn2->SetCommitTimeStamp(6);
  ASSERT_OK(s);
  
  s = txn3->SetCommitTimeStamp(7);
  ASSERT_OK(s);
  
  s = txn2->Commit();
  ASSERT_OK(s);
  s = txn3->Commit();
  ASSERT_OK(s);

  s = txn4->SetCommitTimeStamp(8);
  ASSERT_OK(s);
  s = txn4->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 4);

  s = txn->Rollback();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 8);

  delete txn_ori;  
  delete txn;
  delete txn2;
  delete txn3;
  delete txn4;
}

TEST_F(TOTransactionTest, AdvanceTSAndCleanInLock) {
  WriteOptions write_options;
  ReadOptions read_options;
  RocksTimeStamp all_committed_ts;
  RocksTimeStamp maxToCleanTs = 0;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 3, false);
  ASSERT_OK(s);
  TOTransaction* txn_ori = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn_ori);
  s = txn_ori->SetCommitTimeStamp(6);
  ASSERT_OK(s);
  s = txn_ori->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 6);

  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);
 
  TOTransaction* txn4 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn4);

  s = txn->SetReadTimeStamp(0);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

  s = txn->SetReadTimeStamp(3);
  ASSERT_OK(s);

  s = txn->SetCommitTimeStamp(8);
  ASSERT_OK(s);

  s = txn2->SetReadTimeStamp(5);
  ASSERT_OK(s);  

  s = txn2->SetCommitTimeStamp(7);
  ASSERT_OK(s);

  s = txn3->SetReadTimeStamp(6);
  ASSERT_OK(s);  

  s = txn3->SetCommitTimeStamp(9);
  ASSERT_OK(s);
  
  s = txn2->Commit();
  ASSERT_OK(s);
  ((TOTransactionDBImpl*)txn_db)->AdvanceTS(&maxToCleanTs);

  ASSERT_EQ(maxToCleanTs, 3);
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 7);

  s = txn->Commit();
  ASSERT_OK(s);
  ((TOTransactionDBImpl*)txn_db)->AdvanceTS(&maxToCleanTs);
  ASSERT_EQ(maxToCleanTs, 3);

  s = txn_db->SetTimeStamp(kOldest, 7, false);
  ASSERT_OK(s);

  ((TOTransactionDBImpl*)txn_db)->AdvanceTS(&maxToCleanTs);
  ASSERT_EQ(maxToCleanTs, 6);
  
  s = txn3->Commit();
  ASSERT_OK(s);

  ((TOTransactionDBImpl*)txn_db)->AdvanceTS(&maxToCleanTs);
  ASSERT_EQ(maxToCleanTs, 7);

  s = txn4->SetCommitTimeStamp(10);
  ASSERT_OK(s);
  s = txn4->SetCommitTimeStamp(11);
  ASSERT_OK(s);
  s = txn4->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 11);

  delete txn_ori;  
  delete txn;
  delete txn2;
  delete txn3;
  delete txn4;
}

TEST_F(TOTransactionTest, ThreadsTest) {
	
  WriteOptions write_options;
  ReadOptions read_options;
  string value;

  std::vector<TOTransaction*> txns(31);

  for (uint32_t i = 0; i < 31; i++) {
    txns[i] = txn_db->BeginTransaction(write_options, txn_options);
	ASSERT_TRUE(txns[i]);
	auto s = txns[i]->SetCommitTimeStamp(i+1);
    ASSERT_OK(s);
  }
  
  std::vector<port::Thread> threads;
  for (uint32_t i = 0; i < 31; i++) {
    std::function<void()> blocking_thread = [&, i] {
      auto s = txns[i]->Put(ToString(i + 1), ToString(i + 1));
      ASSERT_OK(s);
	  
	  //printf("threads %d\n",i);
	  s = txns[i]->Commit();
	  ASSERT_OK(s);
      delete txns[i];
    };
    threads.emplace_back(blocking_thread);
  }
  
  //printf("start to join\n");
	
  for (auto& t : threads) {
    t.join();
  }
}

TEST_F(TOTransactionTest, MultiCommitTs) {
  WriteOptions write_options;
  ReadOptions read_options;
  RocksTimeStamp all_committed_ts;
  string value;
  Status s;

  TOTransaction* txn_ori = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn_ori);
  s = txn_ori->SetCommitTimeStamp(4);
  ASSERT_OK(s);
  s = txn_ori->Commit();
  ASSERT_OK(s);
  delete txn_ori;

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 4);

  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn != nullptr);

  s = txn->SetCommitTimeStamp(6); 
  ASSERT_OK(s);

  // commit ts can not set back
  s = txn->SetCommitTimeStamp(5); 
  ASSERT_FALSE(s.ok());
  s = txn->Put("a", "aa");
  ASSERT_OK(s);
  s = txn->SetCommitTimeStamp(7); 
  ASSERT_OK(s);
  s = txn->Put("b", "bb");
  ASSERT_OK(s);
  ASSERT_OK(txn->Commit());
  delete txn;

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2 != nullptr);
  txn2->SetReadTimeStamp(7);
  ASSERT_OK(txn2->Get(read_options, "b", &value));
  ASSERT_EQ(value, "bb");
  ASSERT_OK(txn2->Get(read_options, "a", &value));
  ASSERT_EQ(value, "aa");
  delete txn2;

  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3 != nullptr);
  txn3->SetReadTimeStamp(6);
  ASSERT_OK(txn3->Get(read_options, "a", &value));
  ASSERT_EQ(value, "aa");
  ASSERT_FALSE(txn3->Get(read_options, "b", &value).ok());
  delete txn3;

  TOTransaction* txn4 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn4 != nullptr);
  txn4->SetReadTimeStamp(4);
  ASSERT_FALSE(txn4->Get(read_options, "b", &value).ok());
  ASSERT_FALSE(txn4->Get(read_options, "a", &value).ok());
  delete txn4;
}

TEST_F(TOTransactionTest, PORT_WT_TEST_TIMESTAMP14_TEST_ALL_DURABLE) {
  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;
  RocksTimeStamp all_committed_ts;

  // Since this is a non-prepared transaction, we'll be using the commit
  // timestamp when calculating all_durable since it's implied that they're
  // the same thing.
  auto txn1 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn1->SetCommitTimeStamp(3));
  ASSERT_OK(txn1->Commit());
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 3);

  // We have a running transaction with a lower commit_timestamp than we've
  // seen before. So all_durable should return (lowest commit timestamp - 1).
  txn1 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn1->SetCommitTimeStamp(2));
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 1);
  ASSERT_OK(txn1->Commit());

  // After committing, go back to the value we saw previously.
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 3);

  // For prepared transactions, we take into account the durable timestamp
  // when calculating all_durable.
  txn1 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn1->SetPrepareTimeStamp(6));
  ASSERT_OK(txn1->Prepare());
  ASSERT_OK(txn1->SetCommitTimeStamp(7));
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 3);
  ASSERT_OK(txn1->SetDurableTimeStamp(8));
  ASSERT_OK(txn1->Commit());
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 8);

  // All durable moves back when we have a running prepared transaction
  // with a lower durable timestamp than has previously been committed.
  txn1 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn1->SetPrepareTimeStamp(3));
  ASSERT_OK(txn1->Prepare());
  // If we have a commit timestamp for a prepared transaction, then we
  // don't want that to be visible in the all_durable calculation.
  ASSERT_OK(txn1->SetCommitTimeStamp(4));
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 8);

  // Now take into account the durable timestamp.
  ASSERT_OK(txn1->SetDurableTimeStamp(5));
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 4);
  ASSERT_OK(txn1->Commit());
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 8);

  // Now test a scenario with multiple commit timestamps for a single txn.
  txn1 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn1->SetCommitTimeStamp(6));
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 5);

  // Make more changes and set a new commit timestamp.
  // Our calculation should use the first commit timestamp so there should
  // be no observable difference to the all_durable value.
  ASSERT_OK(txn1->SetCommitTimeStamp(7));
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 5);

  // Once committed, we go back to 8.
  ASSERT_OK(txn1->Commit());
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 8);
}

TEST_F(TOTransactionTest, PORT_WT_TEST_TIMESTAMP14_TEST_ALL_DURABLE_OLD) {
  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;

  // Scenario 0: No commit timestamp has ever been specified therefore
  // There is no all_committed timestamp and we will get an error
  // Querying for it.
  auto txn1 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn1->Commit());
  RocksTimeStamp all_committed_ts;
  ASSERT_TRUE(
      txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts).IsNotFound());

  // Scenario 1: A single transaction with a commit timestamp, will
  // result in the all_durable timestamp being set.
  txn1 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn1->SetCommitTimeStamp(1));
  ASSERT_OK(txn1->Commit());
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 1);

  // Scenario 2: A transaction begins and specifies that it intends
  // to commit at timestamp 2, a second transaction begins and commits
  // at timestamp 3.
  txn1 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn1->SetCommitTimeStamp(2));

  auto txn2 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn2->SetCommitTimeStamp(3));
  ASSERT_OK(txn2->Commit());

  // As the original transaction is still running the all_commit
  // timestamp is being held at 1.
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 1);
  ASSERT_OK(txn1->Commit());

  // Now that the original transaction has finished the all_commit
  // timestamp has moved to 3, skipping 2 as there is a commit with
  // a greater timestamp already existing.
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 3);

  // Senario 3: Commit with a commit timestamp of 5 and then begin a
  // transaction intending to commit at 4, the all_commit timestamp
  // should move back to 3. Until the transaction at 4 completes.
  txn1 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn1->SetCommitTimeStamp(5));
  ASSERT_OK(txn1->Commit());
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 5);

  txn1 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  // All committed will now move back to 3 as it is the point at which
  // all transactions up to that point have committed.
  txn1->SetCommitTimeStamp(4);
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 3);
  ASSERT_OK(txn1->Commit());

  // Now that the transaction at timestamp 4 has completed the
  // all committed timestamp is back at 5.
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 5);

  // Scenario 4: Holding a transaction open without a commit timestamp
  // Will not affect the all_durable timestamp.
  txn1 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  txn2 = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn2->SetCommitTimeStamp(6));
  ASSERT_OK(txn2->Commit());
  ASSERT_OK(txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts));
  ASSERT_EQ(all_committed_ts, 6);
  ASSERT_OK(txn1->Commit());
}

TEST_F(TOTransactionTest, PrepareCommitPointRead) {
  auto db_imp = dynamic_cast<TOTransactionDBImpl*>(txn_db);
  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;
  auto txnW = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnW->Put("abc", "abc"));
  ASSERT_OK(txnW->Get(read_options, "abc", &value));
  ASSERT_EQ(value, "abc");
  ASSERT_OK(txnW->SetPrepareTimeStamp(100));
  ASSERT_OK(txnW->Prepare());
  // NOTE: Get/Put is not allowed after Prepare
  ASSERT_NOK(txnW->Get(read_options, "abc", &value));
  ASSERT_NOK(txnW->Put("abc", "abc"));

  auto txn1 = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  auto txn2 = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  auto txn3 = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn1->SetReadTimeStamp(99));
  ASSERT_OK(txn2->SetReadTimeStamp(101));
  ASSERT_OK(txn3->SetReadTimeStamp(103));
  ASSERT_TRUE(txn1->Get(read_options, "abc", &value).IsNotFound());
  ASSERT_TRUE(txn2->Get(read_options, "abc", &value).IsPrepareConflict());
  ASSERT_TRUE(txn3->Get(read_options, "abc", &value).IsPrepareConflict());
  ASSERT_NOK(txnW->SetCommitTimeStamp(99));
  ASSERT_OK(txnW->SetCommitTimeStamp(102));
  ASSERT_OK(txnW->Commit());
  ASSERT_TRUE(txn1->Get(read_options, "abc", &value).IsNotFound());
  ASSERT_TRUE(txn2->Get(read_options, "abc", &value).IsNotFound());
  ASSERT_OK(txn3->Get(read_options, "abc", &value));
  ASSERT_EQ(value, "abc");
}

TEST_F(TOTransactionTest, PrepareRollbackPointRead) {
  auto db_imp = dynamic_cast<TOTransactionDBImpl*>(txn_db);
  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;
  auto txnW = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnW->Put("abc", "abc"));
  ASSERT_OK(txnW->Get(read_options, "abc", &value));
  ASSERT_EQ(value, "abc");
  ASSERT_OK(txnW->SetPrepareTimeStamp(100));
  ASSERT_OK(txnW->Prepare());
  // NOTE: Get/Put is not allowed after Prepare
  ASSERT_NOK(txnW->Get(read_options, "abc", &value));
  ASSERT_NOK(txnW->Put("abc", "abc"));

  auto txn1 = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  auto txn2 = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  auto txn3 = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn1->SetReadTimeStamp(99));
  ASSERT_OK(txn2->SetReadTimeStamp(101));
  ASSERT_TRUE(txn1->Get(read_options, "abc", &value).IsNotFound());
  ASSERT_TRUE(txn2->Get(read_options, "abc", &value).IsPrepareConflict());
  ASSERT_TRUE(txn3->Get(read_options, "abc", &value).IsPrepareConflict());
  ASSERT_NOK(txnW->SetCommitTimeStamp(99));
  ASSERT_OK(txnW->SetCommitTimeStamp(102));
  ASSERT_OK(txnW->Rollback());
  ASSERT_TRUE(txn1->Get(read_options, "abc", &value).IsNotFound());
  ASSERT_TRUE(txn2->Get(read_options, "abc", &value).IsNotFound());
  ASSERT_TRUE(txn3->Get(read_options, "abc", &value).IsNotFound());
}

TEST_F(TOTransactionTest, PrepareIteratorSameKey) {
  auto db_imp = dynamic_cast<TOTransactionDBImpl*>(txn_db);
  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;
  auto txnW = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnW->Put("abc", "abc"));
  ASSERT_OK(txnW->SetCommitTimeStamp(100));
  ASSERT_OK(txnW->Commit());

  auto txnW1 = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnW1->Put("abc", "def"));
  ASSERT_OK(txnW1->SetPrepareTimeStamp(101));
  ASSERT_OK(txnW1->Prepare());

  auto txnR = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  txnR->SetReadTimeStamp(102);
  auto iter = std::unique_ptr<Iterator>(txnR->GetIterator(read_options));
  ASSERT_TRUE(iter != nullptr);
  iter->Seek("");
  ASSERT_TRUE(iter->status().IsPrepareConflict());

  auto txnR1 = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  txnR1->SetReadTimeStamp(101);
  auto iter1 = std::unique_ptr<Iterator>(txnR1->GetIterator(read_options));
  ASSERT_TRUE(iter1 != nullptr);
  iter1->Seek("");
  ASSERT_TRUE(iter1->status().IsPrepareConflict());

  ASSERT_OK(txnW1->SetCommitTimeStamp(102));
  txnW1->Commit();

  iter->Seek("");
  ASSERT_OK(iter->status());
  ASSERT_EQ(iter->key(), "abc");
  ASSERT_EQ(iter->value(), "def");

  iter1->Seek("");
  ASSERT_OK(iter1->status());
  ASSERT_EQ(iter1->key(), "abc");
  ASSERT_EQ(iter1->value(), "abc");
}

TEST_F(TOTransactionTest, PORT_WT_TEST_PREPARE_05) {
  WriteOptions write_options;
  ReadOptions read_options;
  read_options.read_timestamp = 50;
  string value;
  Status s;

  ASSERT_OK(txn_db->SetTimeStamp(kOldest, 2));
  auto txn = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_NOK(txn->SetPrepareTimeStamp(1));

  // Check setting the prepare timestamp same as oldest timestamp is valid.
  txn = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn->SetPrepareTimeStamp(2));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->SetCommitTimeStamp(3));
  ASSERT_OK(txn->SetDurableTimeStamp(3));
  ASSERT_OK(txn->Commit());

  // In a single transaction it is illegal to set a commit timestamp
  // before invoking prepare for this transaction.
  // Note: Values are not important, setting commit timestamp before
  // prepare itself is illegal.
  txn = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn->SetCommitTimeStamp(3));
  ASSERT_NOK(txn->SetCommitTimeStamp(2));

  txn = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  auto txnR = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR->SetReadTimeStamp(4));
  ASSERT_NOK(txn->SetPrepareTimeStamp(4));
  ASSERT_OK(txn->SetPrepareTimeStamp(5));

  txn = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn->SetPrepareTimeStamp(5));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->SetCommitTimeStamp(5));
  ASSERT_OK(txn->SetDurableTimeStamp(5));
  ASSERT_OK(txn->Commit());
}

TEST_F(TOTransactionTest, PORT_WT_TEST_PREPARE_06) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  ASSERT_OK(txn_db->SetTimeStamp(kOldest, 20));
  auto txn = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_NOK(txn->SetPrepareTimeStamp(10));

  TOTransactionOptions new_txn_options;
  new_txn_options.timestamp_round_read = false;
  new_txn_options.timestamp_round_prepared = true;
  // Check setting the prepare timestamp same as oldest timestamp is valid.
  txn = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, new_txn_options));
  ASSERT_OK(txn->SetPrepareTimeStamp(10));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->SetCommitTimeStamp(15));
  ASSERT_OK(txn->SetDurableTimeStamp(35));
  ASSERT_OK(txn->Commit());

  // Check the cases with an active reader.
  // Start a new reader to have an active read timestamp.
  auto txnR = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR->SetReadTimeStamp(40));
  txn = std::unique_ptr<TOTransaction>(
      txn_db->BeginTransaction(write_options, txn_options));
  // It is illegal to set the prepare timestamp as earlier than an active
  // read timestamp even with roundup_timestamps settings.  This is only
  // checked in diagnostic builds.
  ASSERT_NOK(txn->SetPrepareTimeStamp(10));
}

TEST_F(TOTransactionTest, PORT_WT_TEST_PREPARE_CURSOR_01) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;
  auto db_imp = dynamic_cast<TOTransactionDBImpl*>(txn_db);

  [&] {
    auto tmp_txn = std::unique_ptr<TOTransaction>(
        db_imp->BeginTransaction(write_options, txn_options));
    ASSERT_OK(tmp_txn->Put(Slice("45"), Slice("45")));
    ASSERT_OK(tmp_txn->Put(Slice("46"), Slice("46")));
    ASSERT_OK(tmp_txn->Put(Slice("47"), Slice("47")));
    ASSERT_OK(tmp_txn->Put(Slice("48"), Slice("48")));
    ASSERT_OK(tmp_txn->Put(Slice("49"), Slice("49")));
    ASSERT_OK(tmp_txn->Put(Slice("50"), Slice("50")));
    ASSERT_OK(tmp_txn->Commit());
  }();

  auto txn_prepare = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  auto iter_prepare =
      std::unique_ptr<Iterator>(txn_prepare->GetIterator(read_options));

  // Scenario-1 : Check cursor navigate with insert in prepared transaction.
  // Begin of Scenario-1.
  // Data set at start has keys {2,3,4 ... 50}
  // Insert key 51 to check next operation.
  // Insert key 1 to check prev operation.
  ASSERT_OK(txn_prepare->Put("51", "51"));
  ASSERT_OK(txn_prepare->SetPrepareTimeStamp(100));
  ASSERT_OK(txn_prepare->Prepare());

  // Txn for timestamped reads before prepare timestamp.
  auto txnR_before_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_before_ts->SetReadTimeStamp(50));
  auto iter_before_ts =
      std::unique_ptr<Iterator>(txnR_before_ts->GetIterator(read_options));

  // Txn for timestamped reads between prepare timestamp and commit timestamp.
  auto txnR_between_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_between_ts->SetReadTimeStamp(150));
  auto iter_between_ts =
      std::unique_ptr<Iterator>(txnR_between_ts->GetIterator(read_options));

  // Txn for timestamped reads after commit timestamp.
  auto txnR_after_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_after_ts->SetReadTimeStamp(250));
  auto iter_after_ts =
      std::unique_ptr<Iterator>(txnR_after_ts->GetIterator(read_options));

  // Point all cursors to key 50.
  iter_before_ts->Seek("50");
  iter_between_ts->Seek("50");
  iter_after_ts->Seek("50");

  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_TRUE(iter_after_ts->Valid());

  iter_before_ts->Next();
  ASSERT_FALSE(iter_before_ts->Valid());
  ASSERT_OK(iter_before_ts->status());

  iter_between_ts->Next();
  ASSERT_TRUE(iter_between_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_between_ts->Valid());

  iter_between_ts->Prev();
  ASSERT_OK(iter_between_ts->status());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_EQ(iter_between_ts->key(), Slice("50"));

  iter_after_ts->Next();
  ASSERT_TRUE(iter_after_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_after_ts->Valid());

  ASSERT_OK(txn_prepare->SetCommitTimeStamp(200));
  ASSERT_OK(txn_prepare->SetDurableTimeStamp(200));
  ASSERT_OK(txn_prepare->Commit());

  iter_after_ts->Next();
  ASSERT_OK(iter_after_ts->status());
  ASSERT_TRUE(iter_after_ts->Valid());
  ASSERT_EQ(iter_after_ts->key(), "51");

  ASSERT_OK(txnR_before_ts->Commit());
  ASSERT_OK(txnR_between_ts->Commit());
  ASSERT_OK(txnR_after_ts->Commit());

  // Insert key 44 to check prev operation
  txn_prepare = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn_prepare->Put("44", "44"));
  ASSERT_OK(txn_prepare->SetPrepareTimeStamp(100));
  ASSERT_OK(txn_prepare->Prepare());

  txnR_before_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_before_ts->SetReadTimeStamp(50));
  iter_before_ts =
      std::unique_ptr<Iterator>(txnR_before_ts->GetIterator(read_options));

  txnR_between_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_between_ts->SetReadTimeStamp(150));
  iter_between_ts =
      std::unique_ptr<Iterator>(txnR_between_ts->GetIterator(read_options));

  txnR_after_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_after_ts->SetReadTimeStamp(250));
  iter_after_ts =
      std::unique_ptr<Iterator>(txnR_after_ts->GetIterator(read_options));

  // Point all cursors to key 45.
  iter_before_ts->Seek("45");
  iter_between_ts->Seek("45");
  iter_after_ts->Seek("45");

  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_TRUE(iter_after_ts->Valid());

  // As read is before prepare timestamp, prev is not found.
  iter_before_ts->Prev();
  ASSERT_FALSE(iter_before_ts->Valid());
  ASSERT_OK(iter_before_ts->status());

  // As read is between, prev will point to prepared update.
  iter_between_ts->Prev();
  ASSERT_TRUE(iter_between_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_between_ts->Valid());

  // Check to see next works when a prev returns prepare conflict.
  iter_between_ts->Next();
  ASSERT_OK(iter_between_ts->status());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_EQ(iter_between_ts->key(), Slice("45"));

  // As read is after, prev will point to prepared update.
  iter_after_ts->Prev();
  ASSERT_TRUE(iter_after_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_after_ts->Valid());

  // Commit the prepared transaction.
  ASSERT_OK(txn_prepare->SetCommitTimeStamp(200));
  ASSERT_OK(txn_prepare->SetDurableTimeStamp(200));
  ASSERT_OK(txn_prepare->Commit());

  iter_after_ts->Prev();
  ASSERT_OK(iter_after_ts->status());
  ASSERT_TRUE(iter_after_ts->Valid());
  ASSERT_EQ(iter_after_ts->key(), "44");

  // TODO: it may not meet mongodb's requirements
  // here we advance oldest to clean prepare_map
  ASSERT_OK(txn_db->SetTimeStamp(kOldest, 201));
  ASSERT_OK(txnR_before_ts->Commit());
  ASSERT_OK(txnR_between_ts->Commit());
  ASSERT_OK(txnR_after_ts->Commit());
  // End of Scenario-1.

  // sleep(1) to ensure purged
  sleep(1);
  // Scenario-2 : Check cursor navigate with update in prepared transaction.
  // Begin of Scenario-2.
  // Data set at start has keys {44, 45, 46, 47, 48, 49, 50, 51}
  // Update key 51 to check next operation.
  // Update key 44 to check prev operation.
  txn_prepare = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn_prepare->Put("51", "151"));
  ASSERT_OK(txn_prepare->SetPrepareTimeStamp(300));
  ASSERT_OK(txn_prepare->Prepare());

  txnR_before_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_before_ts->SetReadTimeStamp(250));
  iter_before_ts =
      std::unique_ptr<Iterator>(txnR_before_ts->GetIterator(read_options));

  txnR_between_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_between_ts->SetReadTimeStamp(350));
  iter_between_ts =
      std::unique_ptr<Iterator>(txnR_between_ts->GetIterator(read_options));

  txnR_after_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_after_ts->SetReadTimeStamp(450));
  iter_after_ts =
      std::unique_ptr<Iterator>(txnR_after_ts->GetIterator(read_options));

  // Point all cursors to key 51.
  iter_before_ts->Seek("50");
  iter_between_ts->Seek("50");
  iter_after_ts->Seek("50");

  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_TRUE(iter_after_ts->Valid());

  iter_before_ts->Next();
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_OK(iter_before_ts->status());
  // As read is before prepare timestamp, next is found with previous value.
  ASSERT_EQ(iter_before_ts->key(), "51");
  ASSERT_EQ(iter_before_ts->value(), "51");

  // As read is between, next will point to prepared update.
  iter_between_ts->Next();
  ASSERT_TRUE(iter_between_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_between_ts->Valid());

  // As read is after, next will point to prepared update.
  iter_after_ts->Next();
  ASSERT_TRUE(iter_after_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_after_ts->Valid());

  // Commit the prepared transaction.
  ASSERT_OK(txn_prepare->SetCommitTimeStamp(400));
  ASSERT_OK(txn_prepare->SetDurableTimeStamp(400));
  ASSERT_OK(txn_prepare->Commit());

  // Check to see before cursor still gets the old value.
  iter_before_ts->Seek("51");
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_OK(iter_before_ts->status());
  ASSERT_EQ(iter_before_ts->key(), "51");
  ASSERT_EQ(iter_before_ts->value(), "51");

  // As read is between(i.e before commit), next is not found.
  iter_between_ts->Next();
  ASSERT_OK(iter_between_ts->status());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_EQ(iter_between_ts->key(), "51");
  ASSERT_EQ(iter_between_ts->value(), "51");

  // As read is after, next will point to new key 51.
  iter_after_ts->Next();
  ASSERT_OK(iter_after_ts->status());
  ASSERT_TRUE(iter_after_ts->Valid());
  ASSERT_EQ(iter_after_ts->key(), "51");
  ASSERT_EQ(iter_after_ts->value(), "151");

  ASSERT_OK(txnR_before_ts->Commit());
  ASSERT_OK(txnR_between_ts->Commit());
  ASSERT_OK(txnR_after_ts->Commit());

  // Update key 44 to check prev operation.
  txn_prepare = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn_prepare->Put("44", "444"));
  ASSERT_OK(txn_prepare->SetPrepareTimeStamp(300));
  ASSERT_OK(txn_prepare->Prepare());

  txnR_before_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_before_ts->SetReadTimeStamp(250));
  iter_before_ts =
      std::unique_ptr<Iterator>(txnR_before_ts->GetIterator(read_options));

  txnR_between_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_between_ts->SetReadTimeStamp(350));
  iter_between_ts =
      std::unique_ptr<Iterator>(txnR_between_ts->GetIterator(read_options));

  txnR_after_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_after_ts->SetReadTimeStamp(450));
  iter_after_ts =
      std::unique_ptr<Iterator>(txnR_after_ts->GetIterator(read_options));

  // Check the visibility of new update of prepared transaction.
  // Point all cursors to key 45.
  iter_before_ts->Seek("45");
  iter_between_ts->Seek("45");
  iter_after_ts->Seek("45");

  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_TRUE(iter_after_ts->Valid());

  // As read is before prepare timestamp, prev is not found.
  iter_before_ts->Prev();
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_OK(iter_before_ts->status());
  ASSERT_EQ(iter_before_ts->key(), "44");
  ASSERT_EQ(iter_before_ts->value(), "44");

  // As read is between, prev will point to prepared update.
  iter_between_ts->Prev();
  ASSERT_TRUE(iter_between_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_between_ts->Valid());

  // As read is after, prev will point to prepared update.
  iter_after_ts->Prev();
  ASSERT_TRUE(iter_after_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_after_ts->Valid());

  // Commit the prepared transaction.
  ASSERT_OK(txn_prepare->SetCommitTimeStamp(400));
  ASSERT_OK(txn_prepare->SetDurableTimeStamp(400));
  ASSERT_OK(txn_prepare->Commit());

  // Check to see before cursor still gets the old value.
  iter_before_ts->Seek("44");
  ASSERT_OK(iter_before_ts->status());
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_EQ(iter_before_ts->key(), "44");
  ASSERT_EQ(iter_before_ts->value(), "44");

  // As read is between(i.e before commit), next is not found.
  iter_between_ts->Prev();
  ASSERT_OK(iter_between_ts->status());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_EQ(iter_between_ts->key(), "44");
  ASSERT_EQ(iter_between_ts->value(), "44");

  // As read is after, next will point to new key 44.
  iter_after_ts->Prev();
  ASSERT_OK(iter_after_ts->status());
  ASSERT_TRUE(iter_after_ts->Valid());
  ASSERT_EQ(iter_after_ts->key(), "44");
  ASSERT_EQ(iter_after_ts->value(), "444");

  // End of Scenario-2.
  // TODO: it may not meet mongodb's requirements
  // here we advance oldest to clean prepare_map
  ASSERT_OK(txn_db->SetTimeStamp(kOldest, 401));

  ASSERT_OK(txnR_before_ts->Commit());
  ASSERT_OK(txnR_between_ts->Commit());
  ASSERT_OK(txnR_after_ts->Commit());

  // sleep(1) to ensure purged
  sleep(1);
  // Scenario-3 : Check cursor navigate with remove in prepared transaction.
  // Begin of Scenario-3.
  // Data set at start has keys {44, 45, 46, 47, 48, 49, 50, 51}
  // Remove key 51 to check next operation.
  // Remove key 44 to check prev operation.
  txn_prepare = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn_prepare->Delete("51"));
  ASSERT_OK(txn_prepare->SetPrepareTimeStamp(500));
  ASSERT_OK(txn_prepare->Prepare());

  txnR_before_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_before_ts->SetReadTimeStamp(450));
  iter_before_ts =
      std::unique_ptr<Iterator>(txnR_before_ts->GetIterator(read_options));

  txnR_between_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_between_ts->SetReadTimeStamp(550));
  iter_between_ts =
      std::unique_ptr<Iterator>(txnR_between_ts->GetIterator(read_options));

  txnR_after_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_after_ts->SetReadTimeStamp(650));
  iter_after_ts =
      std::unique_ptr<Iterator>(txnR_after_ts->GetIterator(read_options));

  // Point all cursors to key 51.
  iter_before_ts->Seek("50");
  iter_between_ts->Seek("50");
  iter_after_ts->Seek("50");

  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_TRUE(iter_after_ts->Valid());

  iter_before_ts->Next();
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_OK(iter_before_ts->status());
  // As read is before prepare timestamp, next is found with previous value.
  ASSERT_EQ(iter_before_ts->key(), "51");
  ASSERT_EQ(iter_before_ts->value(), "151");

  // As read is between, next will point to prepared update.
  iter_between_ts->Next();
  ASSERT_TRUE(iter_between_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_between_ts->Valid());

  // As read is after, next will point to prepared update.
  iter_after_ts->Next();
  ASSERT_TRUE(iter_after_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_after_ts->Valid());

  // Commit the prepared transaction.
  ASSERT_OK(txn_prepare->SetCommitTimeStamp(600));
  ASSERT_OK(txn_prepare->SetDurableTimeStamp(600));
  ASSERT_OK(txn_prepare->Commit());

  // Check to see before cursor still gets the old value.
  iter_before_ts->Seek("51");
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_OK(iter_before_ts->status());
  ASSERT_EQ(iter_before_ts->key(), "51");
  ASSERT_EQ(iter_before_ts->value(), "151");

  // As read is between(i.e before commit), next is not found.
  iter_between_ts->Next();
  ASSERT_OK(iter_between_ts->status());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_EQ(iter_between_ts->key(), "51");
  ASSERT_EQ(iter_between_ts->value(), "151");

  // As read is after, next will not be found.
  iter_after_ts->Next();
  ASSERT_OK(iter_after_ts->status());
  ASSERT_FALSE(iter_after_ts->Valid());

  ASSERT_OK(txnR_before_ts->Commit());
  ASSERT_OK(txnR_between_ts->Commit());
  ASSERT_OK(txnR_after_ts->Commit());

  // remove key 44 to check prev operation.
  txn_prepare = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn_prepare->Delete("44"));
  ASSERT_OK(txn_prepare->SetPrepareTimeStamp(500));
  ASSERT_OK(txn_prepare->Prepare());

  txnR_before_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_before_ts->SetReadTimeStamp(450));
  iter_before_ts =
      std::unique_ptr<Iterator>(txnR_before_ts->GetIterator(read_options));

  txnR_between_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_between_ts->SetReadTimeStamp(550));
  iter_between_ts =
      std::unique_ptr<Iterator>(txnR_between_ts->GetIterator(read_options));

  txnR_after_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_after_ts->SetReadTimeStamp(650));
  iter_after_ts =
      std::unique_ptr<Iterator>(txnR_after_ts->GetIterator(read_options));

  // Check the visibility of new update of prepared transaction.
  // Point all cursors to key 45.
  iter_before_ts->Seek("45");
  iter_between_ts->Seek("45");
  iter_after_ts->Seek("45");

  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_TRUE(iter_after_ts->Valid());

  // As read is before prepare timestamp, prev is not found.
  iter_before_ts->Prev();
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_OK(iter_before_ts->status());
  ASSERT_EQ(iter_before_ts->key(), "44");
  ASSERT_EQ(iter_before_ts->value(), "444");

  // As read is between, prev will point to prepared update.
  iter_between_ts->Prev();
  ASSERT_TRUE(iter_between_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_between_ts->Valid());

  // As read is after, prev will point to prepared update.
  iter_after_ts->Prev();
  ASSERT_TRUE(iter_after_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_after_ts->Valid());

  // Commit the prepared transaction.
  ASSERT_OK(txn_prepare->SetCommitTimeStamp(600));
  ASSERT_OK(txn_prepare->SetDurableTimeStamp(600));
  ASSERT_OK(txn_prepare->Commit());

  // Check to see before cursor still gets the old value.
  iter_before_ts->Seek("44");
  ASSERT_OK(iter_before_ts->status());
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_EQ(iter_before_ts->key(), "44");
  ASSERT_EQ(iter_before_ts->value(), "444");

  // As read is between(i.e before commit), next is not found.
  iter_between_ts->Prev();
  ASSERT_OK(iter_between_ts->status());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_EQ(iter_between_ts->key(), "44");
  ASSERT_EQ(iter_between_ts->value(), "444");

  // As read is after, next will point to new key 44.
  iter_after_ts->Prev();
  ASSERT_OK(iter_after_ts->status());
  ASSERT_FALSE(iter_after_ts->Valid());

  // End of Scenario-3.
  // TODO: it may not meet mongodb's requirements
  // here we advance oldest to clean prepare_map
  ASSERT_OK(txn_db->SetTimeStamp(kOldest, 601));
  ASSERT_OK(txnR_before_ts->Commit());
  ASSERT_OK(txnR_between_ts->Commit());
  ASSERT_OK(txnR_after_ts->Commit());

  // Scenario-4 : Check cursor navigate with remove in prepared transaction.
  // remove keys not in the ends.
  // Begin of Scenario-4.
  // Data set at start has keys {45,46,47,48,49,50}
  // Remove key 49 to check next operation.
  // Remove key 46 to check prev operation.
  txn_prepare = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn_prepare->Delete("49"));
  ASSERT_OK(txn_prepare->SetPrepareTimeStamp(700));
  ASSERT_OK(txn_prepare->Prepare());

  txnR_before_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_before_ts->SetReadTimeStamp(650));
  iter_before_ts =
      std::unique_ptr<Iterator>(txnR_before_ts->GetIterator(read_options));

  txnR_between_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_between_ts->SetReadTimeStamp(750));
  iter_between_ts =
      std::unique_ptr<Iterator>(txnR_between_ts->GetIterator(read_options));

  txnR_after_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_after_ts->SetReadTimeStamp(850));
  iter_after_ts =
      std::unique_ptr<Iterator>(txnR_after_ts->GetIterator(read_options));

  // Point all cursors to key 48.
  iter_before_ts->Seek("48");
  iter_between_ts->Seek("48");
  iter_after_ts->Seek("48");

  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_TRUE(iter_after_ts->Valid());

  iter_before_ts->Next();
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_OK(iter_before_ts->status());
  // As read is before prepare timestamp, next is found with 49.
  ASSERT_EQ(iter_before_ts->key(), "49");

  // As read is between, next will point to prepared update.
  iter_between_ts->Next();
  ASSERT_TRUE(iter_between_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_between_ts->Valid());

  // As read is after, next will point to prepared update.
  iter_after_ts->Next();
  ASSERT_TRUE(iter_after_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_after_ts->Valid());

  // Commit the prepared transaction.
  ASSERT_OK(txn_prepare->SetCommitTimeStamp(800));
  ASSERT_OK(txn_prepare->SetDurableTimeStamp(800));
  ASSERT_OK(txn_prepare->Commit());

  // Check to see before cursor still gets the old value.
  iter_before_ts->Seek("49");
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_OK(iter_before_ts->status());
  ASSERT_EQ(iter_before_ts->key(), "49");

  // As read is between(i.e before commit), next is not found.
  iter_between_ts->Next();
  ASSERT_OK(iter_between_ts->status());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_EQ(iter_between_ts->key(), "49");

  // As read is after, next will point beyond end.
  iter_after_ts->Next();
  ASSERT_OK(iter_after_ts->status());
  ASSERT_TRUE(iter_after_ts->Valid());
  ASSERT_EQ(iter_after_ts->key(), "50");

  ASSERT_OK(txnR_before_ts->Commit());
  ASSERT_OK(txnR_between_ts->Commit());
  ASSERT_OK(txnR_after_ts->Commit());

  // remove key 46 to check prev operation.
  txn_prepare = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txn_prepare->Delete("46"));
  ASSERT_OK(txn_prepare->SetPrepareTimeStamp(700));
  ASSERT_OK(txn_prepare->Prepare());

  txnR_before_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_before_ts->SetReadTimeStamp(650));
  iter_before_ts =
      std::unique_ptr<Iterator>(txnR_before_ts->GetIterator(read_options));

  txnR_between_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_between_ts->SetReadTimeStamp(750));
  iter_between_ts =
      std::unique_ptr<Iterator>(txnR_between_ts->GetIterator(read_options));

  txnR_after_ts = std::unique_ptr<TOTransaction>(
      db_imp->BeginTransaction(write_options, txn_options));
  ASSERT_OK(txnR_after_ts->SetReadTimeStamp(850));
  iter_after_ts =
      std::unique_ptr<Iterator>(txnR_after_ts->GetIterator(read_options));

  // Check the visibility of new update of prepared transaction.
  // Point all cursors to key 45.
  iter_before_ts->Seek("47");
  iter_between_ts->Seek("47");
  iter_after_ts->Seek("47");

  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_TRUE(iter_after_ts->Valid());

  // As read is before prepare timestamp, prev is not found.
  iter_before_ts->Prev();
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_OK(iter_before_ts->status());
  ASSERT_EQ(iter_before_ts->key(), "46");

  // As read is between, prev will point to prepared update.
  iter_between_ts->Prev();
  ASSERT_TRUE(iter_between_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_between_ts->Valid());

  // As read is after, prev will point to prepared update.
  iter_after_ts->Prev();
  ASSERT_TRUE(iter_after_ts->status().IsPrepareConflict());
  ASSERT_TRUE(iter_after_ts->Valid());

  // Commit the prepared transaction.
  ASSERT_OK(txn_prepare->SetCommitTimeStamp(800));
  ASSERT_OK(txn_prepare->SetDurableTimeStamp(800));
  ASSERT_OK(txn_prepare->Commit());

  // Check to see before cursor still gets the old value.
  iter_before_ts->Seek("46");
  ASSERT_OK(iter_before_ts->status());
  ASSERT_TRUE(iter_before_ts->Valid());
  ASSERT_EQ(iter_before_ts->key(), "46");

  // As read is between(i.e before commit), next is not found.
  iter_between_ts->Prev();
  ASSERT_OK(iter_between_ts->status());
  ASSERT_TRUE(iter_between_ts->Valid());
  ASSERT_EQ(iter_between_ts->key(), "46");

  // As read is after, next will point to new key 45.
  iter_after_ts->Prev();
  ASSERT_OK(iter_after_ts->status());
  ASSERT_TRUE(iter_after_ts->Valid());
  ASSERT_EQ(iter_after_ts->key(), "45");
}

TEST_F(TOTransactionTest, MemUsage) {
  auto db_imp = dynamic_cast<TOTransactionDBImpl*>(txn_db);
  db_imp->SetMaxConflictBytes(33);
  WriteOptions write_options;
  ReadOptions read_options;
  TOTransactionStat stat;

  TOTransaction* txn = db_imp->BeginTransaction(write_options, txn_options);
  // key(3) + cfid(4) +  txnid(8) = 15
  ASSERT_OK(txn->Put("abc", "abc"));
  memset(&stat, 0, sizeof stat);
  db_imp->Stat(&stat);
  ASSERT_EQ(stat.uk_num, 1);
  ASSERT_EQ(stat.cur_conflict_bytes, 15);
  // 15+16=31
  ASSERT_OK(txn->Put("defg", "defg"));
  memset(&stat, 0, sizeof stat);
  db_imp->Stat(&stat);
  ASSERT_EQ(stat.uk_num, 2);
  ASSERT_EQ(stat.cur_conflict_bytes, 31);
  auto s = txn->Put("h", "h");
  ASSERT_FALSE(s.ok());
  db_imp->SetMaxConflictBytes(50);
  // 31+13=44
  ASSERT_OK(txn->Put("h", "h"));
  memset(&stat, 0, sizeof stat);
  db_imp->Stat(&stat);
  ASSERT_EQ(stat.uk_num, 3);
  ASSERT_EQ(stat.cur_conflict_bytes, 44);
  ASSERT_OK(txn->Commit());
  memset(&stat, 0, sizeof stat);
  db_imp->Stat(&stat);
  ASSERT_EQ(stat.uk_num, 0);
  ASSERT_EQ(stat.ck_num, 3);
  ASSERT_EQ(stat.cur_conflict_bytes, 3 + 28 + 4 + 28 + 1 + 28);
  delete txn;
}


TEST_F(TOTransactionTest, tsPinBottomLevelCompaction) {
  // bottom-level files may contain deletions due to snapshots protecting the
  // deleted keys. Once the snapshot is released, we should see files with many
  // such deletions undergo single-file compactions.
  const int kNumKeysPerFile = 1024;
  const int kNumLevelFiles = 4;
  const int kValueSize = 128;
  auto newOptions = options;
  newOptions.compression = kNoCompression;
  newOptions.level0_file_num_compaction_trigger = kNumLevelFiles;
  // inflate it a bit to account for key/metadata overhead
  newOptions.target_file_size_base = 130 * kNumKeysPerFile * kValueSize / 100;
  Reopen(newOptions);
  auto txn_db_imp = dynamic_cast<TOTransactionDBImpl*>(txn_db);
  auto db_imp = txn_db_imp->getDbImpl();

  WriteOptions write_options;
  ReadOptions read_options;
  //read_options.read_timestamp = 50;
  string value;
  Status s;

  Random rnd(301);
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNumLevelFiles; ++i) {
    TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          txn->Put(DBTestBase::Key(i * kNumKeysPerFile + j), DBTestBase::RandomString(&rnd, kValueSize)));
    }
    if (i == kNumLevelFiles - 1) {
      snapshot = db_imp->GetSnapshot();
      // delete every other key after grabbing a snapshot, so these deletions
      // and the keys they cover can't be dropped until after the snapshot is
      // released.
      for (int j = 0; j < kNumLevelFiles * kNumKeysPerFile; j += 2) {
        ASSERT_OK(txn->Delete(DBTestBase::Key(j)));
      }
    }
    ASSERT_OK(txn->SetCommitTimeStamp(i+1));
    s = txn->Commit();
    ASSERT_OK(s);
    delete txn;
    db_imp->Flush(FlushOptions());
  }
  db_imp->TEST_WaitForCompact();
  std::string level1FileNum;
  db_imp->GetProperty("rocksdb.num-files-at-level1", &level1FileNum);
  ASSERT_EQ(std::to_string(kNumLevelFiles), level1FileNum);
  std::vector<LiveFileMetaData> pre_release_metadata, post_release_metadata;
  db_imp->GetLiveFilesMetaData(&pre_release_metadata);
  // just need to bump seqnum so ReleaseSnapshot knows the newest key in the SST
  // files does not need to be preserved in case of a future snapshot.
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_OK(txn->Put(DBTestBase::Key(0), "val"));
  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  // set the pin_ts to 1, which will make bottom compact make no progress since 
  // every sst file's max ts is >= 1.
  // if want test Bottom compaction working well, 
  // see CompactBottomLevelFilesWithDeletions
  s = txn_db->SetTimeStamp(kOldest, 1, false);
  ASSERT_OK(s);
  db_imp->ReleaseSnapshot(snapshot);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->compaction_reason() ==
                    CompactionReason::kBottommostFiles);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  db_imp->TEST_WaitForCompact();
  db_imp->GetLiveFilesMetaData(&post_release_metadata);
  ASSERT_TRUE(pre_release_metadata.size() == post_release_metadata.size());

  size_t sum_pre = 0, sum_post = 0;
  for (size_t i = 0; i < pre_release_metadata.size(); ++i) {
    ASSERT_EQ(1, pre_release_metadata[i].level);
    sum_pre += pre_release_metadata[i].size;
  }
  for (size_t i = 0; i < post_release_metadata.size(); ++i) {
    sum_post += post_release_metadata[i].size;
  }
  ASSERT_EQ(sum_post, sum_pre);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(TOTransactionTest, TrimHistoryToStableTs) {
  const int kNumKeysPerFile = 1024;
  const int kNumLevelFiles = 4;
  const int kValueSize = 128;
  auto newOptions = options;
  newOptions.compression = kNoCompression;
  newOptions.level0_file_num_compaction_trigger = kNumLevelFiles + 1;
  // inflate it a bit to account for key/metadata overhead
  newOptions.target_file_size_base = 130 * kNumKeysPerFile * kValueSize / 100;
  Reopen(newOptions);
  auto txn_db_imp = dynamic_cast<TOTransactionDBImpl*>(txn_db);
  auto db_imp = txn_db_imp->getDbImpl();

  WriteOptions write_options;
  string value;
  Status s;

  Random rnd(301);
  for (int i = 0; i < kNumLevelFiles; ++i) {
    TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          txn->Put(DBTestBase::Key(i * kNumKeysPerFile + j), DBTestBase::RandomString(&rnd, kValueSize)));
    }
    ASSERT_OK(txn->SetCommitTimeStamp(i+1));
    s = txn->Commit();
    ASSERT_OK(s);
    delete txn;
    db_imp->Flush(FlushOptions());
  }
  db_imp->TEST_WaitForCompact();
  std::string level1FileNum;
  db_imp->GetProperty("rocksdb.num-files-at-level0", &level1FileNum);
  ASSERT_EQ(std::to_string(kNumLevelFiles), level1FileNum);
  std::vector<LiveFileMetaData> pre_release_metadata, post_release_metadata;
  db_imp->GetLiveFilesMetaData(&pre_release_metadata);
  s = txn_db->SetTimeStamp(kStable, 1, false);
  db_imp->TrimHistoryToStableTs(db_imp->DefaultColumnFamily());
  db_imp->GetProperty("rocksdb.num-files-at-level0", &level1FileNum);
  ASSERT_EQ("1", level1FileNum);
    
  db_imp->GetLiveFilesMetaData(&post_release_metadata);
  ASSERT_TRUE(1 == post_release_metadata.size());

  size_t sum_pre = 0, sum_post = 0;
  for (size_t i = 0; i < pre_release_metadata.size(); ++i) {
    ASSERT_EQ(0, pre_release_metadata[i].level);
    sum_pre += pre_release_metadata[i].size;
  }
  for (size_t i = 0; i < post_release_metadata.size(); ++i) {
    ASSERT_EQ(0, post_release_metadata[i].level);
    sum_post += post_release_metadata[i].size;
  }
  ASSERT_LT(sum_post, sum_pre);
}


TEST_F(TOTransactionTest, tsPinBottomLevelCompaction) {
  // bottom-level files may contain deletions due to snapshots protecting the
  // deleted keys. Once the snapshot is released, we should see files with many
  // such deletions undergo single-file compactions.
  const int kNumKeysPerFile = 1024;
  const int kNumLevelFiles = 4;
  const int kValueSize = 128;
  auto newOptions = options;
  newOptions.compression = kNoCompression;
  newOptions.level0_file_num_compaction_trigger = kNumLevelFiles;
  // inflate it a bit to account for key/metadata overhead
  newOptions.target_file_size_base = 130 * kNumKeysPerFile * kValueSize / 100;
  Reopen(newOptions);
  auto txn_db_imp = dynamic_cast<TOTransactionDBImpl*>(txn_db);
  auto db_imp = txn_db_imp->getDbImpl();

  WriteOptions write_options;
  ReadOptions read_options;
  //read_options.read_timestamp = 50;
  string value;
  Status s;

  Random rnd(301);
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNumLevelFiles; ++i) {
    TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          txn->Put(DBTestBase::Key(i * kNumKeysPerFile + j), DBTestBase::RandomString(&rnd, kValueSize)));
    }
    if (i == kNumLevelFiles - 1) {
      snapshot = db_imp->GetSnapshot();
      // delete every other key after grabbing a snapshot, so these deletions
      // and the keys they cover can't be dropped until after the snapshot is
      // released.
      for (int j = 0; j < kNumLevelFiles * kNumKeysPerFile; j += 2) {
        ASSERT_OK(txn->Delete(DBTestBase::Key(j)));
      }
    }
    ASSERT_OK(txn->SetCommitTimeStamp(i+1));
    s = txn->Commit();
    ASSERT_OK(s);
    delete txn;
    db_imp->Flush(FlushOptions());
  }
  db_imp->TEST_WaitForCompact();
  std::string level1FileNum;
  db_imp->GetProperty("rocksdb.num-files-at-level1", &level1FileNum);
  ASSERT_EQ(std::to_string(kNumLevelFiles), level1FileNum);
  std::vector<LiveFileMetaData> pre_release_metadata, post_release_metadata;
  db_imp->GetLiveFilesMetaData(&pre_release_metadata);
  // just need to bump seqnum so ReleaseSnapshot knows the newest key in the SST
  // files does not need to be preserved in case of a future snapshot.
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_OK(txn->Put(DBTestBase::Key(0), "val"));
  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  // set the pin_ts to 1, which will make bottom compact make no progress since 
  // every sst file's max ts is >= 1.
  // if want test Bottom compaction working well, 
  // see CompactBottomLevelFilesWithDeletions
  s = txn_db->SetTimeStamp(kOldest, 1, false);
  ASSERT_OK(s);
  db_imp->ReleaseSnapshot(snapshot);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->compaction_reason() ==
                    CompactionReason::kBottommostFiles);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  db_imp->TEST_WaitForCompact();
  db_imp->GetLiveFilesMetaData(&post_release_metadata);
  ASSERT_TRUE(pre_release_metadata.size() == post_release_metadata.size());

  size_t sum_pre = 0, sum_post = 0;
  for (size_t i = 0; i < pre_release_metadata.size(); ++i) {
    ASSERT_EQ(1, pre_release_metadata[i].level);
    sum_pre += pre_release_metadata[i].size;
  }
  for (size_t i = 0; i < post_release_metadata.size(); ++i) {
    sum_post += post_release_metadata[i].size;
  }
  ASSERT_EQ(sum_post, sum_pre);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(TOTransactionTest, TrimHistoryToStableTs) {
  const int kNumKeysPerFile = 1024;
  const int kNumLevelFiles = 4;
  const int kValueSize = 128;
  auto newOptions = options;
  newOptions.compression = kNoCompression;
  newOptions.level0_file_num_compaction_trigger = kNumLevelFiles + 1;
  // inflate it a bit to account for key/metadata overhead
  newOptions.target_file_size_base = 130 * kNumKeysPerFile * kValueSize / 100;
  Reopen(newOptions);
  auto txn_db_imp = dynamic_cast<TOTransactionDBImpl*>(txn_db);
  auto db_imp = txn_db_imp->getDbImpl();

  WriteOptions write_options;
  string value;
  Status s;

  Random rnd(301);
  for (int i = 0; i < kNumLevelFiles; ++i) {
    TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          txn->Put(DBTestBase::Key(i * kNumKeysPerFile + j), DBTestBase::RandomString(&rnd, kValueSize)));
    }
    ASSERT_OK(txn->SetCommitTimeStamp(i+1));
    s = txn->Commit();
    ASSERT_OK(s);
    delete txn;
    db_imp->Flush(FlushOptions());
  }
  db_imp->TEST_WaitForCompact();
  std::string level1FileNum;
  db_imp->GetProperty("rocksdb.num-files-at-level0", &level1FileNum);
  ASSERT_EQ(std::to_string(kNumLevelFiles), level1FileNum);
  std::vector<LiveFileMetaData> pre_release_metadata, post_release_metadata;
  db_imp->GetLiveFilesMetaData(&pre_release_metadata);
  s = txn_db->SetTimeStamp(kStable, 1, false);
  db_imp->TrimHistoryToStableTs(db_imp->DefaultColumnFamily());
  db_imp->GetProperty("rocksdb.num-files-at-level0", &level1FileNum);
  ASSERT_EQ("1", level1FileNum);
    
  db_imp->GetLiveFilesMetaData(&post_release_metadata);
  ASSERT_TRUE(1 == post_release_metadata.size());

  size_t sum_pre = 0, sum_post = 0;
  for (size_t i = 0; i < pre_release_metadata.size(); ++i) {
    ASSERT_EQ(0, pre_release_metadata[i].level);
    sum_pre += pre_release_metadata[i].size;
  }
  for (size_t i = 0; i < post_release_metadata.size(); ++i) {
    ASSERT_EQ(0, post_release_metadata[i].level);
    sum_post += post_release_metadata[i].size;
  }
  ASSERT_LT(sum_post, sum_pre);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(
      stderr,
      "SKIPPED as optimistic_transaction is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
