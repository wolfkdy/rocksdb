// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include "rocksdb/options.h"
#include "port/port.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/totransaction_db.h"


namespace rocksdb {

class DB;
class Random64;

// Utility class for stress testing transactions.  Can be used to write many
// transactions in parallel and then validate that the data written is logically
// consistent.  This class assumes the input DB is initially empty.
//
// Each call to TransactionDBInsert()/OptimisticTransactionDBInsert() will
// increment the value of a key in #num_sets sets of keys.  Regardless of
// whether the transaction succeeds, the total sum of values of keys in each
// set is an invariant that should remain equal.
//
// After calling TransactionDBInsert()/OptimisticTransactionDBInsert() many
// times, Verify() can be called to validate that the invariant holds.
//
// To test writing Transaction in parallel, multiple threads can create a
// RandomTransactionInserter with similar arguments using the same DB.
class RandomTransactionInserter {
 public:
  // num_keys is the number of keys in each set.
  // num_sets is the number of sets of keys.
  explicit RandomTransactionInserter(
      Random64* rand, const WriteOptions& write_options = WriteOptions(),
      const ReadOptions& read_options = ReadOptions(), uint64_t num_keys = 1000,
      uint16_t num_sets = 3, int readpercent = 0, int deletepercent = 0, 
      int conflict_level_ = 0);
  ~RandomTransactionInserter();

  // Increment a key in each set using a Transaction on a TransactionDB.
  //
  // Returns true if the transaction succeeded OR if any error encountered was
  // expected (eg a write-conflict). Error status may be obtained by calling
  // GetLastStatus();
  bool TransactionDBInsert(
      TransactionDB* db,
      const TransactionOptions& txn_options = TransactionOptions());

  // Increment a key in each set using a Transaction on a TOTransactionDB.
  //
  // Returns true if the transaction succeeded OR if any error encountered was
  // expected (eg a write-conflict). Error status may be obtained by calling
  // GetLastStatus();
  bool TOTransactionDBInsert(
      TOTransactionDB* db);
  
  bool TOTransactionDBWriteRandom(
      std::vector<ColumnFamilyHandle*> handles, TOTransactionDB* db);	  

  // Increment a key in each set using a Transaction on an
  // OptimisticTransactionDB
  //
  // Returns true if the transaction succeeded OR if any error encountered was
  // expected (eg a write-conflict). Error status may be obtained by calling
  // GetLastStatus();
  bool OptimisticTransactionDBInsert(
      OptimisticTransactionDB* db,
      const OptimisticTransactionOptions& txn_options =
          OptimisticTransactionOptions());
  // Increment a key in each set without using a transaction.  If this function
  // is called in parallel, then Verify() may fail.
  //
  // Returns true if the write succeeds.
  // Error status may be obtained by calling GetLastStatus().
  bool DBInsert(DB* db);

  // Get the ikey'th key from set set_i
  static Status DBGet(DB* db, Transaction* txn, ReadOptions& read_options,
                      uint16_t set_i, uint64_t ikey, bool get_for_update,
                      uint64_t* int_value, std::string* full_key,
                      bool* unexpected_error);
  
  static Status DBGet(DB* db, TOTransaction* txn, ReadOptions& read_options,
                      uint16_t set_i, uint64_t ikey, 
                      uint64_t* int_value, std::string* full_key,
                      bool* unexpected_error);

  // Returns OK if Invariant is true.
  static Status Verify(DB* db, uint16_t num_sets, uint64_t num_keys_per_set = 0,
                       bool take_snapshot = false, Random64* rand = nullptr);

  // Returns the status of the previous Insert operation
  Status GetLastStatus() { return last_status_; }

  // Returns the number of successfully written calls to
  // TransactionDBInsert/OptimisticTransactionDBInsert/DBInsert
  uint64_t GetSuccessCount() { return success_count_; }

  // Returns the number of calls to
  // TransactionDBInsert/OptimisticTransactionDBInsert/DBInsert that did not
  // write any data.
  uint64_t GetFailureCount() { return failure_count_; }

  // Returns the sum of user keys/values Put() to the DB.
  size_t GetBytesInserted() { return bytes_inserted_; }

  size_t GetBytesRead() { return bytes_read_; }
  
  int64_t GetFoundCount() { return found_; }
  
  int64_t GetPutCount() { return puts_done_; }
  
  int64_t GetGetCount() { return gets_done_; }
  
  int64_t GetDeleteCount() { return deletes_done_; }

 private:
  // Input options
  Random64* rand_;
  const WriteOptions write_options_;
  ReadOptions read_options_;
  const uint64_t num_keys_;
  const uint16_t num_sets_;
  
  const int readpercent_ ;
  const int deletepercent_ ;
  const int conflict_level_;

  // Number of successful insert batches performed
  uint64_t success_count_ = 0;

  // Number of failed insert batches attempted
  uint64_t failure_count_ = 0;

  size_t bytes_inserted_ = 0;

  
  size_t bytes_read_ = 0;

  const int value_size_ = 1000;

  int64_t found_ = 0;
  int64_t gets_done_ = 0;
  int64_t puts_done_ = 0;
  int64_t deletes_done_ = 0;

  // Status returned by most recent insert operation
  Status last_status_;

  // optimization: re-use allocated transaction objects.
  Transaction* txn_ = nullptr;
  TOTransaction* to_txn_ = nullptr;
  Transaction* optimistic_txn_ = nullptr;

  std::atomic<int> txn_id_;

  bool DoInsert(DB* db, Transaction* txn, bool is_optimistic);
  bool DoInsert(DB* db, TOTransaction* txn);
  bool DoWriteRandom(std::vector<ColumnFamilyHandle*> handles, TOTransaction* txn);
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
