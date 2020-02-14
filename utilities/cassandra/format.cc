// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "format.h"
#include "utilities/cassandra/serialize.h"

namespace rocksdb {
namespace cassandra {
namespace {
const int32_t kDefaultLocalDeletionTime = std::numeric_limits<int32_t>::max();
const int64_t kDefaultMarkedForDeleteAt = std::numeric_limits<int64_t>::min();
const int64_t NO_TIMESTAMP = std::numeric_limits<int64_t>::min();
const int32_t NO_TTL = 0;
const int32_t NO_EXPIRATION_TIME = std::numeric_limits<int32_t>::max();
const int32_t VERSION = 0;
const int64_t NO_LSN = std::numeric_limits<int64_t>::min();
const LivenessInfo EMPTY(0, NO_TIMESTAMP, NO_EXPIRATION_TIME, NO_TTL, NO_LSN);
}


LivenessInfo::LivenessInfo(int8_t flags,
                           int64_t timestamp,
                           int32_t localExpirationTime,
                           int32_t ttl,
                           int64_t lsn) : flags_(flags), timestamp_(timestamp),
                                          localExpirationTime_(
                                            localExpirationTime), ttl_(ttl),
                                          lsn_(lsn) {}

bool LivenessInfo::Supersedes(LivenessInfo other) const {
  if (timestamp_ != other.timestamp_)
    return timestamp_ > other.timestamp_;
  if (IsExpired() ^ other.IsExpired())
    return IsExpired();
  if (IsExpiring() == other.IsExpiring())
    return localExpirationTime_ > other.localExpirationTime_;
  return IsExpiring();
}

bool LivenessInfo::IsExpired() const {
  return ttl_ == NO_EXPIRATION_TIME;
}

bool LivenessInfo::IsExpiring() const {
  return ((u_int8_t) flags_ & HAS_TTL) != 0;
}

int64_t LivenessInfo::Lsn() const {
  return lsn_;
}

int8_t LivenessInfo::Flags() const {
  return flags_;
}


int64_t LivenessInfo::Timestamp() const {
  return timestamp_;
}

int32_t LivenessInfo::LocalExpirationTime() const {
  return localExpirationTime_;
}

int32_t LivenessInfo::Ttl() const {
  return ttl_;
}


ColumnBase::ColumnBase(int64_t lsn, int8_t mask, int32_t index)
  : lsn_(lsn), mask_(mask), index_(index) {}

std::size_t ColumnBase::Size() const {
  return sizeof(mask_) + sizeof(index_);
}

int64_t ColumnBase::Lsn() const {
  return lsn_;
}

int8_t ColumnBase::Mask() const {
  return mask_;
}

int32_t ColumnBase::Index() const {
  return index_;
}


int32_t ColumnBase::PathSize() const {
  return 0;
}

std::size_t ColumnBase::Offset() const {
  return 0;
}


void ColumnBase::Serialize(std::string* dest) const {
  rocksdb::cassandra::Serialize<int8_t>(mask_, dest);
  rocksdb::cassandra::Serialize<int32_t>(index_, dest);
}


std::shared_ptr<ColumnBase> ColumnBase::Deserialize(const char* src,
                                                    std::size_t offset,
                                                    int64_t lsn,
                                                    bool is_complex) {
  auto mask = (u_int8_t) rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  if ((mask & ColumnTypeMask::DELETION_MASK) != 0) {
    return Tombstone::Deserialize(src, offset, lsn, is_complex);
  } else if ((mask & ColumnTypeMask::EXPIRATION_MASK) != 0) {
    return ExpiringColumn::Deserialize(src, offset, lsn, is_complex);
  } else if ((mask & ColumnTypeMask::COMPLEX_CELL_MASK) != 0) {
    return ComplexColumn::Deserialize(src, offset, lsn);
  } else {
    return Column::Deserialize(src, offset, lsn, is_complex);
  }

}

Column::Column(
  int64_t lsn,
  int8_t mask,
  int32_t index,
  int64_t timestamp,
  int32_t path_size,
  int32_t value_size,
  const char* path,
  const char* value
) : ColumnBase(lsn, mask, index), timestamp_(timestamp), path_size_(path_size),
    value_size_(value_size), path_(path), value_(value) {}

int64_t Column::Timestamp() const {
  return timestamp_;
}

std::size_t Column::Size() const {
  return ColumnBase::Size() + sizeof(timestamp_) + sizeof(value_size_) +
         sizeof(path_size_) + value_size_ + path_size_;
}

int32_t Column::PathSize() const {
  return path_size_;
}

const char* Column::Path() const {
  return path_;
}

std::size_t Column::Offset() const {
  return 0;
}

void Column::Serialize(std::string* dest) const {
  ColumnBase::Serialize(dest);
  rocksdb::cassandra::Serialize<int64_t>(timestamp_, dest);
  rocksdb::cassandra::Serialize<int32_t>(path_size_, dest);
  if (path_size_ > 0) {
    dest->append(path_, path_size_);
  }
  rocksdb::cassandra::Serialize<int32_t>(value_size_, dest);
  if (value_size_ > 0) {
    dest->append(value_, value_size_);
  }
}

std::shared_ptr<Column> Column::Deserialize(const char* src,
                                            std::size_t offset,
                                            int64_t lsn,
                                            bool is_complex) {

  int8_t mask = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(mask);
  int32_t index = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(index);
  int64_t timestamp = rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  offset += sizeof(timestamp);
  int32_t path_size = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(path_size);
  const char* path = nullptr;
  if (is_complex) {
    path = src + offset;
    offset += path_size;
  }

  int32_t value_size = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(value_size);
  const char* value = nullptr;
  if (value_size > 0) {
    value = src + offset;
  }
  return std::make_shared<Column>(lsn, mask, index, timestamp, path_size,
                                  value_size, path, value);
}

ExpiringColumn::ExpiringColumn(
  int64_t lsn,
  int8_t mask,
  int32_t index,
  int64_t timestamp,
  int32_t path_size,
  int32_t value_size,
  const char* path,
  const char* value,
  int32_t local_deletion_time,
  int32_t ttl
) : Column(lsn, mask, index, timestamp, path_size, value_size,
           path, value), local_deletion_time_(local_deletion_time), ttl_(ttl) {}

std::size_t ExpiringColumn::Size() const {
  return Column::Size() + sizeof(local_deletion_time_) + sizeof(ttl_);
}

void ExpiringColumn::Serialize(std::string* dest) const {
  Column::Serialize(dest);
  rocksdb::cassandra::Serialize<int32_t>(local_deletion_time_, dest);
  rocksdb::cassandra::Serialize<int32_t>(ttl_, dest);
}

std::chrono::time_point<std::chrono::system_clock>
ExpiringColumn::TimePoint() const {
  return std::chrono::time_point<std::chrono::system_clock>(
    std::chrono::microseconds(Timestamp()));
}


std::chrono::seconds ExpiringColumn::Ttl() const {
  return std::chrono::seconds(ttl_);
}

bool ExpiringColumn::Expired() const {
  return std::chrono::time_point<std::chrono::system_clock>(
    std::chrono::seconds(local_deletion_time_)) <
         std::chrono::system_clock::now();
}

std::shared_ptr<Tombstone> ExpiringColumn::ToTombstone() const {
  int64_t marked_for_delete_at = (int64_t) local_deletion_time_ * 1000000;
  return std::make_shared<Tombstone>(
    Lsn(),
    static_cast<int8_t>(ColumnTypeMask::DELETION_MASK),
    Index(),
    PathSize(),
    local_deletion_time_,
    marked_for_delete_at,
    Path());
}

std::shared_ptr<ExpiringColumn>
ExpiringColumn::Deserialize(const char* src, std::size_t offset, int64_t lsn,
                            bool is_complex) {
  int8_t mask = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(mask);
  int32_t index = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(index);
  int64_t timestamp = rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  offset += sizeof(timestamp);
  int32_t path_size = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(path_size);
  const char* path = nullptr;
  if (is_complex) {
    path = src + offset;
    offset += path_size;
  }
  int32_t value_size = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(value_size);
  const char* value = nullptr;
  if (value_size > 0) {
    value = src + offset;
    offset += value_size;
  }
  int32_t local_deletion_time = rocksdb::cassandra::Deserialize<int32_t>(src,
                                                                         offset);
  offset += sizeof(local_deletion_time);
  int32_t ttl = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  return std::make_shared<ExpiringColumn>(lsn, mask, index, timestamp,
                                          path_size, value_size, path, value,
                                          local_deletion_time, ttl);
}

Tombstone::Tombstone(
  int64_t lsn,
  int8_t mask,
  int32_t index,
  int32_t path_size,
  int32_t local_deletion_time,
  int64_t marked_for_delete_at,
  const char* path
) : ColumnBase(lsn, mask, index), path_size_(path_size),
    local_deletion_time_(local_deletion_time),
    marked_for_delete_at_(marked_for_delete_at), path_(path) {}

int64_t Tombstone::Timestamp() const {
  return marked_for_delete_at_;
}

const char* Tombstone::Path() const {
  return path_;
}

int32_t Tombstone::PathSize() const {
  return path_size_;
}

std::size_t Tombstone::Offset() const {
  return 0;
}

std::size_t Tombstone::Size() const {
  return ColumnBase::Size() + sizeof(local_deletion_time_)
         + sizeof(marked_for_delete_at_) + sizeof(path_size_) + path_size_;
}

void Tombstone::Serialize(std::string* dest) const {
  ColumnBase::Serialize(dest);
  rocksdb::cassandra::Serialize<int32_t>(path_size_, dest);
  rocksdb::cassandra::Serialize<int32_t>(local_deletion_time_, dest);
  rocksdb::cassandra::Serialize<int64_t>(marked_for_delete_at_, dest);
  if (path_size_ > 0) {
    dest->append(path_, path_size_);
  }
}

bool Tombstone::Collectable(std::chrono::seconds gc_grace_period) const {
  auto local_deleted_at = std::chrono::time_point<std::chrono::system_clock>(
    std::chrono::seconds(local_deletion_time_));
  return local_deleted_at + gc_grace_period < std::chrono::system_clock::now();
}

std::shared_ptr<Tombstone> Tombstone::Deserialize(const char* src,
                                                  std::size_t offset,
                                                  int64_t lsn,
                                                  bool is_complex) {
  int8_t mask = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(mask);
  int32_t index = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(index);
  int32_t path_size = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(path_size);
  int32_t local_deletion_time =
    rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(int32_t);
  int64_t marked_for_delete_at =
    rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  offset += sizeof(int64_t);
  const char* path = nullptr;
  if (is_complex) {
    path = src + offset;
  }
  return std::make_shared<Tombstone>(lsn, mask, index, path_size,
                                     local_deletion_time, marked_for_delete_at,
                                     path);
}


ComplexColumn::ComplexColumn(
  int64_t lsn,
  int8_t mask,
  int32_t index,
  int64_t timestamp,
  int32_t local_deletion_time,
  int64_t marked_for_delete_at,
  SubColumnMap columns
) : ColumnBase(lsn, mask, index), timestamp_(timestamp),
    local_deletion_time_(local_deletion_time),
    marked_for_delete_at_(marked_for_delete_at), columns_(std::move(columns)),
    offset_(0) {}

ComplexColumn::ComplexColumn(
  int64_t lsn,
  int8_t mask,
  int32_t index,
  int64_t timestamp,
  int32_t local_deletion_time,
  int64_t marked_for_delete_at,
  SubColumnMap columns,
  std::size_t offset
) : ColumnBase(lsn, mask, index), timestamp_(timestamp),
    local_deletion_time_(local_deletion_time),
    marked_for_delete_at_(marked_for_delete_at), columns_(std::move(columns)),
    offset_(offset) {}

int64_t ComplexColumn::Timestamp() const {
  return timestamp_;
}

std::shared_ptr<ComplexColumn>
ComplexColumn::Merge(const std::shared_ptr<ComplexColumn> columComplex) {

  SubColumnMap columnMap;
  for (auto& pair : this->columns_) {
    columnMap[pair.first] = pair.second;
  }

  int64_t max_timestamp = std::max(timestamp_, columComplex->Timestamp());
  int64_t lsn = std::max(Lsn(), columComplex->Lsn());
  int32_t local_deletion_time = local_deletion_time_;
  int64_t marked_for_delete_at = marked_for_delete_at_;
  if (!this->Supersedes(columComplex->marked_for_delete_at_,
                        columComplex->local_deletion_time_)) {
    local_deletion_time = columComplex->local_deletion_time_;
    marked_for_delete_at = columComplex->marked_for_delete_at_;
  }

  for (auto& pair : columComplex->columns_) {
    SortedKey key = pair.first;
    if (columnMap.find(key) == columnMap.end()) {
      if (pair.second->Timestamp() > marked_for_delete_at ||
          marked_for_delete_at == NO_TIMESTAMP)
        columnMap[pair.first] = pair.second;
    } else {
      if (pair.second->Timestamp() > columnMap[key]->Timestamp()) {
        columnMap[key] = pair.second;
      }
    }
  }
  return std::make_shared<ComplexColumn>(lsn, Mask(), Index(), max_timestamp,
                                         local_deletion_time,
                                         marked_for_delete_at,
                                         columnMap);
}

std::size_t ComplexColumn::Size() const {
  int32_t value_size = 0;
  for (auto& pair : columns_) {
    value_size += pair.second->Size();
  }
  return ColumnBase::Size() + sizeof(local_deletion_time_) +
         sizeof(marked_for_delete_at_) + sizeof(int32_t) + value_size;
}

const char* ComplexColumn::Path() const {
  return nullptr;
}

std::size_t ComplexColumn::Offset() const {
  return offset_;
}


bool ComplexColumn::Supersedes(int64_t markedForDeleteAt,
                               int32_t localDeletionTime) const {

  return marked_for_delete_at_ > markedForDeleteAt ||
         (marked_for_delete_at_ == markedForDeleteAt &&
          local_deletion_time_ > localDeletionTime);
}

void ComplexColumn::Collectable(std::chrono::seconds gc_grace_period) {

  SubColumnMap complex_comlumn;
  for (auto& pair : this->columns_) {
    if (pair.second->Mask() == ColumnTypeMask::DELETION_MASK) {
      std::shared_ptr<Tombstone> tombstone = std::static_pointer_cast<Tombstone>(
        pair.second);
      if (tombstone->Collectable(gc_grace_period)) {
        continue;
      }
    }
    complex_comlumn[pair.first] = pair.second;
  }
  this->columns_ = complex_comlumn;

}

void ComplexColumn::Serialize(std::string* dest) const {
  ColumnBase::Serialize(dest);
  rocksdb::cassandra::Serialize<int32_t>(local_deletion_time_, dest);
  rocksdb::cassandra::Serialize<int64_t>(marked_for_delete_at_, dest);
  rocksdb::cassandra::Serialize<int32_t>(static_cast<int32_t>(columns_.size()), dest);
  for (auto& pair : columns_) {
    pair.second->Serialize(dest);
  }
}

std::shared_ptr<ComplexColumn> ComplexColumn::Deserialize(const char* src,
                                                          std::size_t offset,
                                                          int64_t lsn) {

  int8_t mask = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(mask);
  int32_t index = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(index);
  int32_t local_deletion_time =
    rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(int32_t);
  int64_t marked_for_delete_at =
    rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  offset += sizeof(int64_t);
  int32_t cell_count = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(cell_count);

  SubColumnMap columns;
  int tmp_count = cell_count;
  std::size_t value_size =
    sizeof(mask) + sizeof(index) + sizeof(int32_t) + sizeof(int64_t) +
    sizeof(cell_count);
  int64_t max_timestamp = marked_for_delete_at;
  while (tmp_count > 0) {
    tmp_count--;
    auto c = ColumnBase::Deserialize(src, offset, lsn, true);
    std::size_t column_size = c->Size();
    offset += column_size;
    value_size += column_size;
    columns[SortedKey(c->Path(), c->PathSize(), 0)] = c;
    max_timestamp = std::max(max_timestamp, c->Timestamp());
  }
  return std::make_shared<ComplexColumn>(lsn, mask, index, max_timestamp,
                                         local_deletion_time,
                                         marked_for_delete_at, columns,
                                         value_size);
}

RowValue::RowValue(int32_t version, int64_t lsn, int8_t flags, int32_t ck_size,
                   int32_t local_deletion_time, int64_t marked_for_delete_at,
                   int64_t last_modified_time, int64_t time_stamp,
                   int32_t local_expiration_time, int32_t ttl)
  : version_(version), lsn_(lsn), flags_(flags), ck_size_(ck_size),
    local_deletion_time_(local_deletion_time),
    marked_for_delete_at_(marked_for_delete_at), time_stamp_(time_stamp),
    local_expiration_time_(local_expiration_time), ttl_(ttl), columns_(),
    last_modified_time_(last_modified_time) {}


RowValue::RowValue(Columns columns, int32_t version, int64_t lsn, int8_t flags,
                   int32_t ck_size, int32_t local_deletion_time,
                   int64_t marked_for_delete_at, int64_t last_modified_time,
                   int64_t time_stamp, int32_t local_expiration_time,
                   int32_t ttl)
  : version_(version), lsn_(lsn), flags_(flags), ck_size_(ck_size),
    local_deletion_time_(local_deletion_time),
    marked_for_delete_at_(marked_for_delete_at), time_stamp_(time_stamp),
    local_expiration_time_(local_expiration_time), ttl_(ttl),
    columns_(std::move(columns)), last_modified_time_(last_modified_time) {}

std::size_t RowValue::Size() const {
  std::size_t size =
    sizeof(version_) + sizeof(lsn_) + sizeof(flags_) + sizeof(ck_size_) +
    sizeof(local_deletion_time_) + sizeof(marked_for_delete_at_) +
    sizeof(time_stamp_) + sizeof(local_expiration_time_) + sizeof(ttl_);
  for (const auto& column : columns_) {
    size += column->Size();
  }
  return size;
}

int64_t RowValue::LastModifiedTime() const {
  return last_modified_time_;
}

int64_t RowValue::TimeStamp() const {
  return time_stamp_;
}

int32_t RowValue::Version() const {
  return version_;
}

int64_t RowValue::Lsn() const {
  return lsn_;
}

int8_t RowValue::Flags() const {
  return flags_;
}

int32_t RowValue::CkSize() const {
  return ck_size_;
}

int32_t RowValue::LocalExpirationTime() const {
  return local_expiration_time_;
}

int32_t RowValue::Ttl() const {
  return ttl_;
}

std::chrono::time_point<std::chrono::system_clock>
RowValue::LastModifiedTimePoint() const {
  return std::chrono::time_point<std::chrono::system_clock>(
    std::chrono::microseconds(LastModifiedTime()));
}

int32_t RowValue::LocalDeletionTime() const {
  return local_deletion_time_;
}

int64_t RowValue::MarkedForDeleteAt() const {
  return marked_for_delete_at_;
}

bool RowValue::IsTombstone() const {
  return marked_for_delete_at_ > kDefaultMarkedForDeleteAt &&
         marked_for_delete_at_ > time_stamp_;
}

void RowValue::Serialize(std::string* dest) const {
  rocksdb::cassandra::Serialize<int32_t>(version_, dest);
  rocksdb::cassandra::Serialize<int64_t>(lsn_, dest);
  rocksdb::cassandra::Serialize<int8_t>(flags_, dest);
  rocksdb::cassandra::Serialize<int32_t>(ck_size_, dest);
  rocksdb::cassandra::Serialize<int32_t>(local_deletion_time_, dest);
  rocksdb::cassandra::Serialize<int64_t>(marked_for_delete_at_, dest);
  rocksdb::cassandra::Serialize<int64_t>(time_stamp_, dest);
  rocksdb::cassandra::Serialize<int32_t>(local_expiration_time_, dest);
  rocksdb::cassandra::Serialize<int32_t>(ttl_, dest);
  for (const auto& column : columns_) {
    column->Serialize(dest);
  }
}

RowValue RowValue::RemoveExpiredColumns(bool* changed) const {
  *changed = false;
  Columns new_columns;
  for (auto& column : columns_) {
    if (column->Mask() == ColumnTypeMask::EXPIRATION_MASK) {
      std::shared_ptr<ExpiringColumn> expiring_column =
        std::static_pointer_cast<ExpiringColumn>(column);

      if (expiring_column->Expired()) {
        *changed = true;
        continue;
      }
    }

    new_columns.push_back(column);
  }
  return RowValue(std::move(new_columns), version_, lsn_, flags_, ck_size_,
                  local_deletion_time_, marked_for_delete_at_,
                  last_modified_time_, time_stamp_, local_expiration_time_,
                  ttl_);
}

RowValue RowValue::ConvertExpiredColumnsToTombstones(bool* changed) const {
  *changed = false;
  Columns new_columns;
  for (auto& column : columns_) {
    if (column->Mask() == ColumnTypeMask::EXPIRATION_MASK) {
      std::shared_ptr<ExpiringColumn> expiring_column =
        std::static_pointer_cast<ExpiringColumn>(column);

      if (expiring_column->Expired()) {
        shared_ptr<Tombstone> tombstone = expiring_column->ToTombstone();
        new_columns.push_back(tombstone);
        *changed = true;
        continue;
      }
    }
    new_columns.push_back(column);
  }
  return RowValue(std::move(new_columns), version_, lsn_, flags_, ck_size_,
                  local_deletion_time_, marked_for_delete_at_,
                  last_modified_time_, time_stamp_, local_expiration_time_,
                  ttl_);
}

RowValue RowValue::RemoveTombstones(
  std::chrono::seconds gc_grace_period) const {
  Columns new_columns;
  for (auto& column : columns_) {
    if (column->Mask() == ColumnTypeMask::DELETION_MASK) {
      std::shared_ptr<Tombstone> tombstone =
        std::static_pointer_cast<Tombstone>(column);
      if ((flags_ & FlagsMask::IS_COMPACT) != 0) {
        continue;
      }
      if (tombstone->Collectable(gc_grace_period)) {
        continue;
      }
    } else if (column->Mask() == ColumnTypeMask::COMPLEX_CELL_MASK) {
      std::static_pointer_cast<ComplexColumn>(column)->Collectable(
        gc_grace_period);
    }

    new_columns.push_back(column);
  }
  if (((u_int8_t) flags_ & FlagsMask::IS_COMPACT) != 0 && new_columns.empty()) {
    std::chrono::microseconds micro_seconds(last_modified_time_);
    auto local_deletion_time = (int32_t) (micro_seconds.count() *
                                          std::chrono::microseconds::period::num /
                                          std::chrono::microseconds::period::den);
    return RowValue(version_, lsn_, flags_, ck_size_, local_deletion_time,
                    marked_for_delete_at_, last_modified_time_, time_stamp_,
                    local_expiration_time_, ttl_);
  }
  return RowValue(std::move(new_columns), version_, lsn_, flags_, ck_size_,
                  local_deletion_time_, marked_for_delete_at_,
                  last_modified_time_, time_stamp_, local_expiration_time_,
                  ttl_);
}

bool RowValue::Empty() const {
  return columns_.empty();
}

RowValue RowValue::Deserialize(const char* src, std::size_t size) {
  std::size_t offset = 0;
  assert(size >=
         sizeof(version_) + sizeof(lsn_) + sizeof(flags_) + sizeof(ck_size_) +
         sizeof(local_deletion_time_) + sizeof(marked_for_delete_at_) +
         sizeof(time_stamp_) + sizeof(local_expiration_time_) + sizeof(ttl_));
  int32_t version = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(int32_t);
  int64_t lsn = rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  offset += sizeof(int64_t);
  int8_t flags = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(int8_t);
  int32_t ck_size = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(int32_t);
  int32_t local_deletion_time = rocksdb::cassandra::Deserialize<int32_t>(src,
                                                                         offset);
  offset += sizeof(int32_t);
  int64_t marked_for_delete_at = rocksdb::cassandra::Deserialize<int64_t>(src,
                                                                          offset);
  offset += sizeof(int64_t);
  int64_t time_stamp = rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  offset += sizeof(int64_t);
  int32_t live_ness_ldt = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(int32_t);
  int32_t ttl = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(int32_t);
  if (offset == size) {
    return RowValue(version, lsn, flags, ck_size, local_deletion_time,
                    marked_for_delete_at, time_stamp, time_stamp, live_ness_ldt,
                    ttl);
  }

  Columns columns;
  int64_t last_modified_time = NO_TIMESTAMP;
  while (offset < size) {
    auto mask = (u_int8_t) rocksdb::cassandra::Deserialize<int8_t>(src, offset);
    auto c = ColumnBase::Deserialize(src, offset, lsn, false);
    if ((mask & ColumnTypeMask::COMPLEX_CELL_MASK) != 0) {
      offset += c->Offset();
    } else {
      offset += c->Size();
    }
    assert(offset <= size);
    last_modified_time = std::max(last_modified_time, c->Timestamp());
    columns.push_back(std::move(c));
  }
  return RowValue(std::move(columns), version, lsn, flags, ck_size,
                  local_deletion_time, marked_for_delete_at, last_modified_time,
                  time_stamp, live_ness_ldt, ttl);
}


// Merge multiple row values into one.
// For each column in rows with same index, we pick the one with latest
// timestamp. And we also take row tombstone into consideration, by iterating
// each row from reverse timestamp order, and stop once we hit the first
// row tombstone.
RowValue RowValue::Merge(std::vector<RowValue>&& values) {
  assert(values.size() > 0);
  if (values.size() == 1) {
    return std::move(values[0]);
  }

  // Merge columns by their last modified time, and skip once we hit
  // a row tombstone.
  std::sort(values.begin(), values.end(),
            [](const RowValue& r1, const RowValue& r2) {
              if (r1.LastModifiedTime() == r2.LastModifiedTime()) {
                return r1.Lsn() > r2.Lsn();
              }
              return r1.LastModifiedTime() > r2.LastModifiedTime();
            });

  std::map<int32_t, std::shared_ptr<ColumnBase>> merged_columns;

  LivenessInfo livenessInfo = EMPTY;
  int64_t lsn = NO_LSN;
  int32_t ck_size = 0;
  int64_t mark_delete_at = kDefaultMarkedForDeleteAt;
  int32_t local_delete_time = kDefaultLocalDeletionTime;

  for (auto& value : values) {
    lsn = std::max(lsn, value.Lsn());
    if (value.MarkedForDeleteAt() > mark_delete_at) {
      mark_delete_at = value.MarkedForDeleteAt();
      local_delete_time = value.LocalDeletionTime();
    } else if (value.MarkedForDeleteAt() == mark_delete_at &&
               value.LocalDeletionTime() > local_delete_time) {
      local_delete_time = value.LocalDeletionTime();
    }

    LivenessInfo tmpLiveinf(value.Flags(), value.TimeStamp(),
                            value.LocalExpirationTime(), value.Ttl(),
                            value.Lsn());

    if (tmpLiveinf.Supersedes(livenessInfo)) {
      livenessInfo = tmpLiveinf;
      ck_size = value.CkSize();
    }
    for (auto& column : value.columns_) {
      int32_t index = column->Index();
      if (merged_columns.find(index) == merged_columns.end()) {
        merged_columns[index] = column;
      } else {
        if (column->Mask() == ColumnTypeMask::COMPLEX_CELL_MASK) {
          std::shared_ptr<ComplexColumn> complex_column = std::static_pointer_cast<ComplexColumn>(
            column);
          std::shared_ptr<ComplexColumn> complex_merged = std::static_pointer_cast<ComplexColumn>(
            merged_columns[index]);
          merged_columns[index] = complex_merged->Merge(complex_column);
        } else {
          if (column->Timestamp() > merged_columns[index]->Timestamp()) {
            merged_columns[index] = column;
          }
        }
      }
    }
  }

  int64_t last_modified_time = values[0].LastModifiedTime();
  Columns columns;
  for (auto& pair: merged_columns) {
    if (pair.second->Timestamp() <= mark_delete_at) {
      continue;
    }
    last_modified_time = std::max(last_modified_time, pair.second->Timestamp());
    columns.push_back(std::move(pair.second));
  }
  return RowValue(std::move(columns), VERSION, lsn, livenessInfo.Flags(),
                  ck_size, local_delete_time, mark_delete_at,
                  last_modified_time, livenessInfo.Timestamp(),
                  livenessInfo.LocalExpirationTime(), livenessInfo.Ttl());
}


Columns RowValue::GetColumns() {
  return columns_;
}

const PartitionDeletion PartitionDeletion::kDefault(VERSION, NO_LSN,
                                                    kDefaultLocalDeletionTime,
                                                    kDefaultMarkedForDeleteAt);
const std::size_t PartitionDeletion::kSize =
  sizeof(int32_t) + sizeof(int64_t) + sizeof(int32_t) + sizeof(int64_t);

PartitionDeletion::PartitionDeletion(int32_t version, int64_t lsn,
                                     int32_t local_deletion_time,
                                     int64_t marked_for_delete_at)
  : version_(version), lsn_(lsn), local_deletion_time_(local_deletion_time),
    marked_for_delete_at_(marked_for_delete_at) {}

int32_t PartitionDeletion::Version() const {
  return version_;
}

int64_t PartitionDeletion::Lsn() const {
  return lsn_;
}

int64_t PartitionDeletion::MarkForDeleteAt() const {
  return marked_for_delete_at_;
}

int32_t PartitionDeletion::LocalDeletionTime() const {
  return local_deletion_time_;
}

PartitionDeletion PartitionDeletion::Deserialize(const char* src,
                                                 std::size_t size) {
  if (size < kSize) {
    return PartitionDeletion::kDefault;
  }
  std::size_t offset = 0;
  int32_t version = rocksdb::cassandra::Deserialize<int32_t>(src, 0);
  offset += sizeof(int32_t);
  int64_t lsn = rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  offset += sizeof(int64_t);
  int32_t local_deletion_time =
    rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(int32_t);
  int64_t marked_for_delete_at =
    rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  return PartitionDeletion(version, lsn, local_deletion_time,
                           marked_for_delete_at);
}

void PartitionDeletion::Serialize(std::string* dest) const {
  rocksdb::cassandra::Serialize<int32_t>(version_, dest);
  rocksdb::cassandra::Serialize<int64_t>(lsn_, dest);
  rocksdb::cassandra::Serialize<int32_t>(local_deletion_time_, dest);
  rocksdb::cassandra::Serialize<int64_t>(marked_for_delete_at_, dest);
}

// Merge multiple PartitionDeletion only keep latest one
PartitionDeletion PartitionDeletion::Merge(
  std::vector<PartitionDeletion>&& pds) {
  assert(pds.size() > 0);
  PartitionDeletion candidate = kDefault;
  for (auto& deletion : pds) {
    if (deletion.Supersedes(candidate)) {
      candidate = deletion;
    }
  }
  return candidate;
}

bool PartitionDeletion::Supersedes(PartitionDeletion& pd) const {
  return MarkForDeleteAt() > pd.MarkForDeleteAt() ||
         (MarkForDeleteAt() == pd.MarkForDeleteAt() &&
          LocalDeletionTime() > pd.LocalDeletionTime());
}


PartitionHeader::PartitionHeader(PartitionDeletion partition_deletion,
                                 Columns columns, int32_t version, int64_t lsn,
                                 int8_t flags, int32_t local_deletion_time,
                                 int64_t marked_for_delete_at,
                                 int64_t last_modified_time, int64_t time_stamp,
                                 int32_t live_ness_ldt, int32_t ttl,
                                 Markers markers)
  : RowValue(std::move(columns), version, lsn, flags, 0, local_deletion_time,
             marked_for_delete_at, last_modified_time, time_stamp,
             live_ness_ldt, ttl), partition_deletion_(partition_deletion),
    markers_(std::move(markers)) {}

PartitionHeader::PartitionHeader(PartitionDeletion partition_deletion,
                                 int32_t version, int64_t lsn, int8_t flags,
                                 int32_t local_deletion_time,
                                 int64_t marked_for_delete_at,
                                 int64_t time_stamp, int32_t live_ness_ldt,
                                 int32_t ttl, Markers markers)
  : RowValue(version, lsn, flags, 0, local_deletion_time, marked_for_delete_at,
             time_stamp, time_stamp, live_ness_ldt, ttl),
    partition_deletion_(partition_deletion), markers_(std::move(markers)) {}

PartitionHeader
PartitionHeader::Deserialize(const char* src, std::size_t size) {
  std::size_t offset = 0;
  auto mask = (u_int8_t) rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(int8_t);
  Markers values;
  if ((mask & PartitionHeaderMask::P_DELETION_MASK) != 0) {
    int8_t flags = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
    offset += sizeof(int8_t);
    PartitionDeletion pd = PartitionDeletion::Deserialize(src + offset,
                                                          size - offset);
    return PartitionHeader(pd, VERSION, NO_LSN, flags,
                           kDefaultLocalDeletionTime,
                           kDefaultMarkedForDeleteAt, NO_TIMESTAMP,
                           NO_EXPIRATION_TIME,
                           NO_TTL, std::move(values));
  } else if ((mask & PartitionHeaderMask::STATIC_MASK) != 0) {
    RowValue rv = RowValue::Deserialize(src + offset, size - offset);
    return PartitionHeader(PartitionDeletion::kDefault, rv.GetColumns(),
                           rv.Version(), rv.Lsn(), rv.Flags(),
                           rv.LocalDeletionTime(), rv.MarkedForDeleteAt(),
                           rv.LastModifiedTime(), rv.TimeStamp(),
                           rv.LocalExpirationTime(), rv.Ttl(),
                           std::move(values));
  } else if ((mask & PartitionHeaderMask::RANGE_TOMBSTONE_MASK) != 0) {
    int8_t flags = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
    offset += sizeof(int8_t);
    while (offset < size) {
      auto mark = RangeTombstone::Deserialize(
        src + offset);
      offset += mark->Size();
      values.push_back(std::move(mark));
    }
    return PartitionHeader(PartitionDeletion::kDefault, VERSION, NO_LSN,
                           flags, kDefaultLocalDeletionTime,
                           kDefaultMarkedForDeleteAt, NO_TIMESTAMP,
                           NO_EXPIRATION_TIME, NO_TTL, std::move(values));
  } else {
    PartitionDeletion pd = PartitionDeletion::Deserialize(src + offset,
                                                          size - offset);
    offset += PartitionDeletion::kSize;
    int32_t value_size = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
    offset += sizeof(value_size);
    int32_t markers_size = rocksdb::cassandra::Deserialize<int32_t>(src,
                                                                    offset);
    offset += sizeof(markers_size);
    std::size_t row_value_size = 0;
    row_value_size += value_size;
    RowValue rv = RowValue::Deserialize(src + offset, row_value_size);
    offset += value_size;
    while (offset < size) {
      auto mark = RangeTombstone::Deserialize(
        src + offset);
      offset += mark->Size();
      values.push_back(std::move(mark));
    }
    return PartitionHeader(pd, rv.GetColumns(), rv.Version(), rv.Lsn(),
                           rv.Flags(), rv.LocalDeletionTime(),
                           rv.MarkedForDeleteAt(), rv.LastModifiedTime(),
                           rv.TimeStamp(), rv.LocalExpirationTime(), rv.Ttl(),
                           std::move(values));
  }
}


void PartitionHeader::Serialize(std::string* dest) const {
  rocksdb::cassandra::Serialize<int8_t>(PartitionHeaderMask::ALL_MASK, dest);
  partition_deletion_.Serialize(dest);
  rocksdb::cassandra::Serialize<int32_t>(static_cast<int32_t>(RowValue::Size()), dest);
  int32_t marker_size = 0;
  for (auto& m : markers_) {
    marker_size += m->Size();
  }
  rocksdb::cassandra::Serialize<int32_t>(marker_size, dest);
  RowValue::Serialize(dest);
  for (auto& m : markers_) {
    m->Serialize(dest);
  }
}


RowValue PartitionHeader::ToRowValue() {
  return RowValue(GetColumns(), Version(), Lsn(), Flags(), CkSize(),
                  LocalDeletionTime(), MarkedForDeleteAt(), LastModifiedTime(),
                  TimeStamp(), LocalExpirationTime(), Ttl());
}

PartitionHeader PartitionHeader::Merge(
  std::vector<PartitionHeader>&& phs, std::chrono::seconds gc_grace_period) {
  assert(phs.size() > 0);
  PartitionDeletion candidate = PartitionDeletion::kDefault;
  std::vector<RowValue> row_values;
  Markers markers;
  int8_t flags = 0;
  for (auto& header : phs) {
    if (header.partition_deletion_.Supersedes(candidate)) {
      candidate = header.partition_deletion_;
      flags = header.Flags();
    }
    row_values.push_back(header.ToRowValue());
  }

  for (auto& header : phs) {
    for (auto marker : header.GetMarkers()) {
      if (marker->MarkForDeleteAt() > candidate.MarkForDeleteAt())
        markers.push_back(std::move(marker));
    }
  }

  if (row_values.size() > 0) {
    RowValue merged = RowValue::Merge(std::move(row_values));
    merged = merged.RemoveTombstones(gc_grace_period);
    return PartitionHeader(candidate, merged.GetColumns(),
                           merged.Version(), merged.Lsn(), flags,
                           merged.LocalDeletionTime(),
                           merged.MarkedForDeleteAt(),
                           merged.LastModifiedTime(), merged.TimeStamp(),
                           merged.LocalExpirationTime(), merged.Ttl(),
                           std::move(markers));
  }
  return PartitionHeader(candidate, VERSION, NO_LSN, flags,
                         kDefaultLocalDeletionTime, kDefaultMarkedForDeleteAt,
                         NO_TIMESTAMP, NO_EXPIRATION_TIME, NO_TTL,
                         std::move(markers));
}

std::size_t PartitionHeader::Size() const {
  std::size_t size =
    PartitionDeletion::kSize + sizeof(int8_t) + sizeof(int32_t) +
    sizeof(int32_t);
  size += RowValue::Size();
  for (auto& m : markers_) {
    size += m->Size();
  }
  return size;
}

PartitionDeletion PartitionHeader::GetPD() {
  return partition_deletion_;
}


Markers PartitionHeader::GetMarkers() {
  return markers_;
}

PartitionHeader PartitionHeader::Default() {
  return PartitionHeader(PartitionDeletion::kDefault, VERSION, NO_LSN,
                         int8_t(0), kDefaultLocalDeletionTime,
                         kDefaultMarkedForDeleteAt, NO_TIMESTAMP,
                         NO_EXPIRATION_TIME, NO_TTL, Markers());
}

SortedKey::SortedKey(const char* path, int32_t size, int8_t type) : path_(path),
                                                                    size_(size),
                                                                    type_(
                                                                      type) {}

int SortedKey::Compare(const char* str1, int32_t size_1, const char* str2,
                       int32_t size_2) const {
  return char_compare(str1, size_1, str2, size_2, true);
}

bool SortedKey::operator<(const SortedKey& cmp) const {
  return Compare(path_, size_, cmp.path_, cmp.size_) < 0;
}

RangeTombstone::RangeTombstone(int32_t version, int64_t lsn, int8_t mask,
                               int64_t marked_for_delete_at,
                               int32_t local_deletion_time,
                               int32_t start_ck_size,
                               int8_t start_kind,
                               int32_t start_value_size,
                               const char* start_value,
                               int32_t end_ck_size,
                               int8_t end_kind,
                               int32_t end_value_size,
                               const char* end_value) :
  version_(version),
  lsn_(lsn),
  mask_(mask),
  marked_for_delete_at_(marked_for_delete_at),
  local_deletion_time_(local_deletion_time),
  start_ck_size_(start_ck_size),
  start_kind_(start_kind),
  start_value_size_(start_value_size),
  start_value_(start_value),
  end_ck_size_(end_ck_size),
  end_kind_(end_kind),
  end_value_size_(end_value_size),
  end_value_(end_value) {}

std::size_t RangeTombstone::Size() const {
  return sizeof(version_) + sizeof(lsn_) + sizeof(mask_) +
         sizeof(marked_for_delete_at_) +
         sizeof(local_deletion_time_) + sizeof(start_ck_size_) +
         sizeof(start_kind_) + sizeof(start_value_size_) + start_value_size_ +
         sizeof(end_ck_size_) + sizeof(end_kind_) + sizeof(end_value_size_) +
         end_value_size_;
}

void RangeTombstone::Serialize(std::string* dest) const {
  rocksdb::cassandra::Serialize<int32_t>(version_, dest);
  rocksdb::cassandra::Serialize<int64_t>(lsn_, dest);
  rocksdb::cassandra::Serialize<int8_t>(mask_, dest);
  rocksdb::cassandra::Serialize<int64_t>(marked_for_delete_at_, dest);
  rocksdb::cassandra::Serialize<int32_t>(local_deletion_time_, dest);
  rocksdb::cassandra::Serialize<int32_t>(start_ck_size_, dest);
  rocksdb::cassandra::Serialize<int8_t>(start_kind_, dest);
  rocksdb::cassandra::Serialize<int32_t>(start_value_size_, dest);
  dest->append(start_value_, start_value_size_);
  rocksdb::cassandra::Serialize<int32_t>(end_ck_size_, dest);
  rocksdb::cassandra::Serialize<int8_t>(end_kind_, dest);
  rocksdb::cassandra::Serialize<int32_t>(end_value_size_, dest);
  dest->append(end_value_, end_value_size_);
}

std::shared_ptr<RangeTombstone> RangeTombstone::Deserialize(const char* src) {

  std::size_t offset = 0;
  int32_t version = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(version);
  int64_t lsn = rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  offset += sizeof(lsn);
  int8_t mask = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(mask);
  int64_t marked_for_delete_at = rocksdb::cassandra::Deserialize<int64_t>(src,
                                                                          offset);
  offset += sizeof(marked_for_delete_at);
  int32_t local_deletion_time = rocksdb::cassandra::Deserialize<int32_t>(src,
                                                                         offset);
  offset += sizeof(local_deletion_time);

  int32_t start_ck_size = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(start_ck_size);
  int8_t start_kind = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(int8_t);
  int32_t start_value_size = rocksdb::cassandra::Deserialize<int32_t>(src,
                                                                      offset);
  offset += sizeof(start_value_size);
  const char* start_value = src + offset;
  offset += start_value_size;

  int32_t end_ck_size = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(end_ck_size);
  int8_t end_kind = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(int8_t);
  int32_t end_value_size = rocksdb::cassandra::Deserialize<int32_t>(src,
                                                                    offset);
  offset += sizeof(end_value_size);
  const char* end_value = src + offset;

  return std::make_shared<RangeTombstone>(version, lsn, mask,
                                          marked_for_delete_at,
                                          local_deletion_time,
                                          start_ck_size,
                                          start_kind, start_value_size,
                                          start_value, end_ck_size, end_kind,
                                          end_value_size, end_value);
}

int32_t RangeTombstone::Version() const {
  return version_;
}

int64_t RangeTombstone::Lsn() const {
  return lsn_;
}

int8_t RangeTombstone::Mask() const {
  return mask_;
}

int64_t RangeTombstone::MarkForDeleteAt() const {
  return marked_for_delete_at_;
}

int32_t RangeTombstone::LocalDeletionTime() const {
  return local_deletion_time_;
}

int32_t RangeTombstone::StartCkSize() const {
  return start_ck_size_;
}

int8_t RangeTombstone::StartKind() const {
  return start_kind_;
}

int32_t RangeTombstone::StartValueSize() const {
  return start_value_size_;
}

const char* RangeTombstone::StartData() const {
  return start_value_;
}

int32_t RangeTombstone::EndCkSize() const {
  return end_ck_size_;
}

int8_t RangeTombstone::EndKind() const {
  return end_kind_;
}

int32_t RangeTombstone::EndValueSize() const {
  return end_value_size_;
}

const char* RangeTombstone::EndData() const {
  return end_value_;
}


} // namepsace cassandrda
} // namespace rocksdb
