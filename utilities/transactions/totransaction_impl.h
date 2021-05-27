#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "db/db_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/totransaction.h"
#include "rocksdb/utilities/totransaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"


namespace rocksdb {

using TxnKey = std::pair<uint32_t, std::string>;

// TimeStamp Ordering Transaction Options
struct TOTxnOptions {
  size_t max_write_batch_size = 1000;
  Logger* log_ = nullptr;
};

class TOTransactionDBImpl;

class TOTransactionImpl : public TOTransaction {
 public:
  struct ActiveTxnNode {
    // NOTE(deyukong): txn_id_ is indeed duplicated with txn_snapshot
    // consider using txn_snapshot
    TransactionID txn_id_;
    TransactionID commit_txn_id_;
    bool commit_ts_set_;
    RocksTimeStamp commit_ts_;
    RocksTimeStamp first_commit_ts_;
    bool read_ts_set_;
    RocksTimeStamp read_ts_;
    bool prepare_ts_set_;
    RocksTimeStamp prepare_ts_;
    bool durable_ts_set_;
    RocksTimeStamp durable_ts_;
    bool timestamp_published_;
    bool timestamp_round_prepared_;
    bool timestamp_round_read_;
    bool read_only_;
    bool ignore_prepare_;
    std::atomic<TOTransaction::TOTransactionState> state_;
    const Snapshot* txn_snapshot;
    WriteBatchWithIndex write_batch_;

   public:
    ActiveTxnNode()
        : txn_id_(0),
          commit_txn_id_(0),
          commit_ts_set_(false),
          commit_ts_(0),
          first_commit_ts_(0),
          read_ts_set_(false),
          read_ts_(std::numeric_limits<RocksTimeStamp>::max()),
          prepare_ts_set_(false),
          prepare_ts_(0),
          durable_ts_set_(false),
          durable_ts_(0),
          timestamp_published_(false),
          timestamp_round_prepared_(false),
          timestamp_round_read_(false),
          state_(TOTransaction::kStarted),
          txn_snapshot(nullptr),
          write_batch_(BytewiseComparator(), 0, true /*overwrite_keys*/, 0) {}
  };

  TOTransactionImpl(TOTransactionDB* db,
              const WriteOptions& options, 
              const TOTxnOptions& txn_options,
              const std::shared_ptr<ActiveTxnNode>& core);

  virtual ~TOTransactionImpl();

  virtual Status SetPrepareTimeStamp(const RocksTimeStamp& timestamp) override;

  virtual Status SetCommitTimeStamp(const RocksTimeStamp& timestamp) override;

  virtual Status SetDurableTimeStamp(const RocksTimeStamp& timestamp) override;

  virtual Status SetReadTimeStamp(const RocksTimeStamp& timestamp) override;

  virtual Status SetReadTimeStamp(const RocksTimeStamp& timestamp, const uint32_t& round) override;

  virtual Status GetReadTimeStamp(RocksTimeStamp* timestamp) const override;

  virtual Status Prepare() override;

  virtual Status Commit() override;

  virtual Status Rollback() override;

  virtual Status Get(ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value) override;

  virtual Status Get(ReadOptions& options, const Slice& key,
                     std::string* value) override;

  virtual Iterator* GetIterator(ReadOptions& read_options) override;

  virtual Iterator* GetIterator(ReadOptions& read_options,
                                ColumnFamilyHandle* column_family) override;

  virtual Status Put(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) override;

  virtual Status Put(const Slice& key, const Slice& value) override;

  virtual Status Delete(ColumnFamilyHandle* column_family, const Slice& key) override;

  virtual Status Delete(const Slice& key) override;

  virtual Status SetName(const TransactionName& name) override;

  virtual TransactionID GetID() const override;

  virtual TOTransactionState GetState() const override;

  virtual WriteBatchWithIndex* GetWriteBatch() override;

  const ActiveTxnNode* GetCore() const;

  // Check write conflict. If there is no write conflict, add the key to uncommitted keys
  Status CheckWriteConflict(const TxnKey& key);

  // Generate a new unique transaction identifier
  static TransactionID GenTxnID();

 private:
  // Used to create unique ids for transactions.
  static std::atomic<TransactionID> txn_id_counter_;

  // Unique ID for this transaction
  TransactionID txn_id_;

  // Updated keys in this transaction
  // TODO(deyukong): writtenKeys_ is duplicated with core_->Write_batch_, remove
  // this
  std::set<TxnKey> written_keys_;

  DB* db_;
  DBImpl* db_impl_;
  TOTransactionDBImpl* txn_db_impl_;
  
  WriteOptions write_options_;
  TOTxnOptions txn_option_;

  std::shared_ptr<ActiveTxnNode> core_;  
};

} // namespace rocksdb
#endif
