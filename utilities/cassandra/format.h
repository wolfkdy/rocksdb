// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <chrono>
#include <vector>
#include <memory>
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "util/testharness.h"

namespace rocksdb {
namespace cassandra {

// Identify the type of the column.
enum ColumnTypeMask {
  DELETION_MASK = 0x01,
  EXPIRATION_MASK = 0x02,
  COMPLEX_CELL_MASK = 0x04,
};


enum PartitionHeaderMask {
  P_DELETION_MASK = 0x01,
  STATIC_MASK = 0x02,
  ALL_MASK = 0x04,
  RANGE_TOMBSTONE_MASK = 0x08,
};

enum FlagsMask {
  IS_COMPACT = 0x02,
  HAS_TIMESTAMP = 0x04,
  HAS_TTL = 0x08,
};

enum CK_KIND {
  EXCL_END_BOUND = int8_t(0),
  INCL_START_BOUND = int8_t(1),
  CLUSTERING = int8_t(4),
  INCL_END_BOUND = int8_t(6),
  EXCL_START_BOUND = int8_t(7),
};


class SortedKey {
public:
  SortedKey(const char* path, int32_t size, int8_t type);

  ~SortedKey() = default;

  bool operator<(const SortedKey& cmp) const;

  int Compare(const char* str1, int32_t size_1, const char* str2,
              int32_t size_2) const;

  const char* path_;
  int32_t size_;
  int8_t type_;

  static int long_compare(int64_t x, int64_t y) {
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
  }

  static int int8_t_compare(int8_t x, int8_t y) {
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
  }

  static int8_t comparedToClustering(int8_t kind) {
    switch (kind) {
      case INCL_END_BOUND:
        return 1;
      case EXCL_END_BOUND:
        return -1;
      case INCL_START_BOUND:
        return -1;
      case EXCL_START_BOUND:
        return 1;
      default:
        return 0;
    }
  }

  static int char_compare(const char* str1, int32_t size_1, const char* str2,
                          int32_t size_2, bool is_full) {
    const int32_t min_len = (size_1 < size_2) ? size_1 : size_2;
    int ret = memcmp(str1, str2, min_len);
    if (is_full) {
      if (size_1 == size_2) {
        if (ret < 0)
          return -1;
        else if (ret > 0)
          return 1;
        return 0;
      } else {
        if (ret < 0)
          return -1;
        else if (ret > 0)
          return 1;
        else {
          if (size_1 < size_2) {
            return -1;
          } else {
            return 1;
          }
        }
      }
    } else {
      if (ret < 0)
        return -1;
      else if (ret > 0)
        return 1;
      return 0;
    }
  }

};

struct PartitionValue {
  std::string value;
  size_t pk_length;
};

class LivenessInfo {
public:
  LivenessInfo(int8_t flags, int64_t timestamp, int32_t localExpirationTime,
               int32_t ttl, int64_t lsn);

  bool Supersedes(LivenessInfo other) const;

  bool IsExpired() const;

  bool IsExpiring() const;

  int8_t Flags() const;

  int64_t Timestamp() const;

  int32_t LocalExpirationTime() const;

  int32_t Ttl() const;

  int64_t Lsn() const;

private:
  int8_t flags_;
  int64_t timestamp_;
  int32_t localExpirationTime_;
  int32_t ttl_;
  int64_t lsn_;
};

class ColumnBase {
public:
  ColumnBase(int64_t lsn, int8_t mask, int32_t index);

  virtual ~ColumnBase() = default;

  virtual int64_t Timestamp() const = 0;

  virtual int64_t Lsn() const;

  virtual int8_t Mask() const;

  virtual int32_t Index() const;

  virtual std::size_t Size() const;

  virtual const char* Path() const = 0;

  virtual int32_t PathSize() const;

  virtual std::size_t Offset() const;

  virtual void Serialize(std::string* dest) const;

  static std::shared_ptr<ColumnBase> Deserialize(const char* src,
                                                 std::size_t offset,
                                                 int64_t lsn,
                                                 bool is_complex);

private:
  int64_t lsn_;
  int8_t mask_;
  int32_t index_;

};

class Column : public ColumnBase {
public:
  Column(int64_t lsn, int8_t mask, int32_t index, int64_t timestamp,
         int32_t path_size, int32_t value_size, const char* path,
         const char* value);

  virtual int64_t Timestamp() const override;

  virtual std::size_t Size() const override;

  virtual const char* Path() const override;

  virtual int32_t PathSize() const override;

  virtual void Serialize(std::string* dest) const override;

  virtual std::size_t Offset() const override;

  static std::shared_ptr<Column> Deserialize(const char* src,
                                             std::size_t offset,
                                             int64_t lsn,
                                             bool is_complex);

private:
  int64_t timestamp_;
  int32_t path_size_;
  int32_t value_size_;
  const char* path_;
  const char* value_;
};


typedef std::map<SortedKey, std::shared_ptr<ColumnBase>> SubColumnMap;

class ComplexColumn : public ColumnBase {
public:
  ComplexColumn(int64_t lsn, int8_t mask, int32_t index, int64_t timestamp,
                int32_t local_deletion_time, int64_t marked_for_delete_at,
                SubColumnMap columns);

  ComplexColumn(int64_t lsn, int8_t mask, int32_t index, int64_t timestamp,
                int32_t local_deletion_time, int64_t marked_for_delete_at,
                SubColumnMap columns, std::size_t offset);

  virtual int64_t Timestamp() const override;

  virtual std::size_t Size() const override;

  virtual const char* Path() const override;

  virtual void Serialize(std::string* dest) const override;

  virtual std::size_t Offset() const override;

  bool Supersedes(int64_t markedForDeleteAt, int32_t localDeletionTime) const;

  virtual std::shared_ptr<ComplexColumn>
  Merge(const std::shared_ptr<ComplexColumn> complexColumn);

  void Collectable(std::chrono::seconds gc_grace_period);

  static std::shared_ptr<ComplexColumn> Deserialize(const char* src,
                                                    std::size_t offset,
                                                    int64_t lsn);

private:
  int64_t timestamp_;
  int32_t local_deletion_time_;
  int64_t marked_for_delete_at_;
  SubColumnMap columns_;
  std::size_t offset_;
};

class Tombstone : public ColumnBase {
public:
  Tombstone(int64_t lsn, int8_t mask, int32_t index, int32_t path_size,
            int32_t local_deletion_time, int64_t marked_for_delete_at,
            const char* path);

  virtual int64_t Timestamp() const override;

  virtual std::size_t Size() const override;

  virtual const char* Path() const override;

  virtual int32_t PathSize() const override;

  virtual void Serialize(std::string* dest) const override;

  virtual std::size_t Offset() const override;

  bool Collectable(std::chrono::seconds gc_grace_period) const;

  static std::shared_ptr<Tombstone> Deserialize(const char* src,
                                                std::size_t offset,
                                                int64_t lsn,
                                                bool is_complex);

private:
  int32_t path_size_;
  int32_t local_deletion_time_;
  int64_t marked_for_delete_at_;
  const char* path_;
};


class ExpiringColumn : public Column {
public:
  ExpiringColumn(int64_t lsn, int8_t mask, int32_t index, int64_t timestamp,
                 int32_t path_size, int32_t value_size, const char* path,
                 const char* value, int32_t local_deletion_time, int32_t ttl);

  virtual std::size_t Size() const override;

  virtual void Serialize(std::string* dest) const override;

  bool Expired() const;

  std::shared_ptr<Tombstone> ToTombstone() const;

  static std::shared_ptr<ExpiringColumn> Deserialize(const char* src,
                                                     std::size_t offset,
                                                     int64_t lsn,
                                                     bool is_complex);

private:
  int32_t local_deletion_time_;
  int32_t ttl_;

  std::chrono::time_point<std::chrono::system_clock> TimePoint() const;

  std::chrono::seconds Ttl() const;
};


typedef std::vector<std::shared_ptr<ColumnBase>> Columns;

class RowValue {
public:
  // Create a Row Tombstone.
  RowValue(int32_t version, int64_t lsn, int8_t flags, int32_t ck_size,
           int32_t local_deletion_time, int64_t marked_for_delete_at,
           int64_t last_modified_time, int64_t time_stamp,
           int32_t local_expiration_time, int32_t ttl);

  RowValue(Columns columns, int32_t version, int64_t lsn, int8_t flags,
           int32_t ck_size, int32_t local_deletion_time,
           int64_t marked_for_delete_at, int64_t last_modified_time,
           int64_t time_stamp, int32_t local_expiration_time, int32_t ttl);

  RowValue(const RowValue& /*that*/) = delete;

  RowValue(RowValue&& /*that*/) noexcept = default;

  RowValue& operator=(const RowValue& /*that*/) = delete;

  RowValue& operator=(RowValue&& /*that*/) = default;

  virtual std::size_t Size() const;

  bool IsTombstone() const;

  // For Tombstone this returns the marked_for_delete_at_,
  // otherwise it returns the max timestamp of containing columns.
  int64_t LastModifiedTime() const;

  std::chrono::time_point<std::chrono::system_clock> LastModifiedTimePoint()
  const;

  int32_t LocalDeletionTime() const;

  int64_t MarkedForDeleteAt() const;

  virtual void Serialize(std::string* dest) const;

  RowValue RemoveExpiredColumns(bool* changed) const;

  RowValue ConvertExpiredColumnsToTombstones(bool* changed) const;

  RowValue RemoveTombstones(std::chrono::seconds gc_grace_period) const;

  bool Empty() const;

  int64_t TimeStamp() const;

  int32_t LocalExpirationTime() const;

  int32_t Ttl() const;

  int32_t Version() const;

  int64_t Lsn() const;

  int8_t Flags() const;

  int32_t CkSize() const;

  static RowValue Deserialize(const char* src, std::size_t size);

  // Merge multiple rows according to their timestamp.
  static RowValue Merge(std::vector<RowValue>&& values);

  Columns GetColumns();

  virtual ~RowValue() = default;

private:
  int32_t version_;
  int64_t lsn_;
  int8_t flags_;
  int32_t ck_size_;
  int32_t local_deletion_time_;
  int64_t marked_for_delete_at_;
  int64_t time_stamp_;
  int32_t local_expiration_time_;
  int32_t ttl_;
  Columns columns_;
  int64_t last_modified_time_;


  FRIEND_TEST(RowValueTest, PurgeTtlShouldRemvoeAllColumnsExpired);

  FRIEND_TEST(RowValueTest, ExpireTtlShouldConvertExpiredColumnsToTombstones);

  FRIEND_TEST(RowValueMergeTest, Merge);

  FRIEND_TEST(RowValueMergeTest, MergeWithRowTombstone);

  FRIEND_TEST(CassandraFunctionalTest, SimpleMergeTest);

  FRIEND_TEST(
    CassandraFunctionalTest, CompactionShouldConvertExpiredColumnsToTombstone);

  FRIEND_TEST(
    CassandraFunctionalTest, CompactionShouldPurgeExpiredColumnsIfPurgeTtlIsOn);

  FRIEND_TEST(
    CassandraFunctionalTest,
    CompactionShouldRemoveRowWhenAllColumnExpiredIfPurgeTtlIsOn);

  FRIEND_TEST(CassandraFunctionalTest,
              CompactionShouldRemoveTombstoneExceedingGCGracePeriod);
};

class PartitionDeletion {
public:
  PartitionDeletion(int32_t version, int64_t lsn, int32_t local_deletion_time,
                    int64_t marked_for_delete_at);

  int32_t Version() const;

  int64_t Lsn() const;

  int64_t MarkForDeleteAt() const;

  int32_t LocalDeletionTime() const;

  void Serialize(std::string* dest) const;

  bool Supersedes(PartitionDeletion& pd) const;

  static PartitionDeletion Merge(std::vector<PartitionDeletion>&& pds);

  static PartitionDeletion Deserialize(const char* src, std::size_t size);

  const static PartitionDeletion kDefault;
  const static std::size_t kSize;

private:
  int32_t version_;
  int64_t lsn_;
  int32_t local_deletion_time_;
  int64_t marked_for_delete_at_;
};

class RangeTombstone {
public:
  RangeTombstone(int32_t version, int64_t lsn, int8_t mask,
                 int64_t marked_for_delete_at,
                 int32_t local_deletion_time, int32_t start_ck_size,
                 int8_t start_kind, int32_t start_value_size,
                 const char* start_value, int32_t end_ck_size, int8_t end_kind,
                 int32_t end_value_size, const char* end_value);

  static std::shared_ptr<RangeTombstone> Deserialize(const char* src);

  void Serialize(std::string* dest) const;

  std::size_t Size() const;

  int32_t Version() const;

  int64_t Lsn() const;

  int8_t Mask() const;

  int64_t MarkForDeleteAt() const;

  int32_t LocalDeletionTime() const;

  int32_t StartCkSize() const;

  int8_t StartKind() const;

  int32_t StartValueSize() const;

  const char* StartData() const;

  int32_t EndCkSize() const;

  int8_t EndKind() const;

  int32_t EndValueSize() const;

  const char* EndData() const;

  int Compare(const RangeTombstone& that) const {
    return 0;
  }

  bool operator<(const RangeTombstone& that) const {
    return Compare(that) < 0;
  }

private:
  int32_t version_;
  int64_t lsn_;
  int8_t mask_;
  int64_t marked_for_delete_at_;
  int32_t local_deletion_time_;
  int32_t start_ck_size_;
  int8_t start_kind_;
  int32_t start_value_size_;
  const char* start_value_;
  int32_t end_ck_size_;
  int8_t end_kind_;
  int32_t end_value_size_;
  const char* end_value_;
};

typedef std::vector<std::shared_ptr<RangeTombstone>> Markers;

class PartitionHeader : public RowValue {
public:
  PartitionHeader(PartitionDeletion partition_deletion,
                  Columns columns, int32_t version, int64_t lsn,
                  int8_t flags, int32_t local_deletion_time,
                  int64_t marked_for_delete_at,
                  int64_t last_modified_time, int64_t time_stamp,
                  int32_t live_ness_ldt, int32_t ttl,
                  Markers markers);

  PartitionHeader(PartitionDeletion partition_deletion,
                  int32_t version, int64_t lsn, int8_t flags,
                  int32_t local_deletion_time,
                  int64_t marked_for_delete_at,
                  int64_t time_stamp, int32_t live_ness_ldt,
                  int32_t ttl, Markers markers);

  PartitionHeader(const PartitionHeader& /*that*/) = delete;

  PartitionHeader(PartitionHeader&& /*that*/) noexcept = default;

  static PartitionHeader Default();

  static PartitionHeader Deserialize(const char* src, std::size_t size);

  static PartitionHeader Merge(std::vector<PartitionHeader>&& phs,
                               std::chrono::seconds gc_grace_period);

  virtual void Serialize(std::string* dest) const override;

  virtual std::size_t Size() const override;

  RowValue ToRowValue();

  PartitionDeletion GetPD();

  Markers GetMarkers();

private:
  PartitionDeletion partition_deletion_;
  Markers markers_;
};

} // namepsace cassandrda
} // namespace rocksdb
