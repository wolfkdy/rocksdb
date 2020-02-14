// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/cassandra/cassandra_compaction_filter.h"

namespace rocksdb {
namespace cassandra {

const int64_t NO_TIMESTAMP = std::numeric_limits<int64_t>::min();

const char* CassandraCompactionFilter::Name() const {
  return "CassandraCompactionFilter";
}

void CassandraCompactionFilter::SetMetaCfHandle(
  DB* meta_db, ColumnFamilyHandle* meta_cf_handle) {
  meta_db_ = meta_db;
  meta_cf_handle_ = meta_cf_handle;
}

PartitionValue CassandraCompactionFilter::GetPartitionHeader(
  const Slice& key) const {
  PartitionValue pv;
  if (!meta_db_) {
    // skip triming when parition meta db is not ready yet
    pv.pk_length = 0;
    pv.value = std::string();
    return pv;
  }

  DB* meta_db = meta_db_.load();
  if (!meta_cf_handle_) {
    // skip triming when parition meta cf handle is not ready yet
    pv.pk_length = 0;
    pv.value = std::string();
    return pv;
  }
  ColumnFamilyHandle* meta_cf_handle = meta_cf_handle_.load();
  if (partition_key_length_ > 0) {
    return GetPartitionHeaderByPointQuery(key, meta_db, meta_cf_handle);
  } else {
    return GetPartitionHeaderByScan(key, meta_db, meta_cf_handle);
  }
}

PartitionValue CassandraCompactionFilter::GetPartitionHeaderByPointQuery(
  const Slice& key, DB* meta_db, ColumnFamilyHandle* meta_cf) const {
  std::string val;
  PartitionValue pv;
  if (key.size() < partition_key_length_) {
    pv.value = val;
    pv.pk_length = 0;
    return pv;
  }
  Slice partition_key(key.data(), key.size());
  partition_key.remove_suffix(key.size() - partition_key_length_);
  meta_db->Get(meta_read_options_, meta_cf, partition_key, &val);
  pv.value = val;
  pv.pk_length = partition_key_length_;
  return pv;
}

PartitionValue CassandraCompactionFilter::GetPartitionHeaderByScan(
  const Slice& key, DB* meta_db, ColumnFamilyHandle* meta_cf) const {
  auto it =
    unique_ptr<Iterator>(meta_db->NewIterator(meta_read_options_, meta_cf));
  // partition meta key is encoded token+paritionkey
  it->SeekForPrev(key);
  PartitionValue pv;
  if (!it->Valid()) {
    // skip trimming when
    pv.value = std::string();
    pv.pk_length = 0;
    return pv;
  }

  if (!key.starts_with(it->key())) {
    // skip trimming when there is no parition meta data
    pv.value = std::string();
    pv.pk_length = 0;
    return pv;
  }

  Slice value = it->value();
  pv.value = value.ToString();
  pv.pk_length = it->key().size();
  return pv;
}

bool CassandraCompactionFilter::ShouldDropByParitionHeader(
  const Slice& key,
  std::chrono::time_point<std::chrono::system_clock> row_timestamp,
  int64_t timestamp, int32_t ck_size) const {
  std::chrono::seconds gc_grace_period =
    ignore_range_delete_on_read_ ? std::chrono::seconds(0) : gc_grace_period_;
  PartitionValue pv = GetPartitionHeader(key);
  std::string val = pv.value;
  if (val.size() > 0) {
    PartitionHeader partitionHeader = PartitionHeader::Deserialize(val.data(),
                                                                   val.size());
    bool cmp = false;
    if (partitionHeader.GetPD().MarkForDeleteAt() != NO_TIMESTAMP) {
      cmp = std::chrono::time_point<std::chrono::system_clock>(
        std::chrono::microseconds(
          partitionHeader.GetPD().MarkForDeleteAt())) >
            row_timestamp + gc_grace_period;
    }
    if (cmp) {
      return cmp;
    } else {
      return ShouldDropByMarker(key, partitionHeader.GetMarkers(), timestamp,
                                ck_size, pv.pk_length);
    }
  } else {
    return false;
  }

}

int CassandraCompactionFilter::compare(const char* str1, int32_t size_1,
                                       int8_t kind1, int ck_size1,
                                       const char* str2,
                                       int32_t size_2, int8_t kind2,
                                       int ck_size2) {
  int cmp = SortedKey::char_compare(str1, size_1, str2, size_2,
                                    ck_size1 == ck_size2);
  if (cmp != 0)
    return cmp;
  if (ck_size1 == ck_size2)
    return SortedKey::int8_t_compare(kind1, kind2);
  return ck_size1 < ck_size2 ? SortedKey::comparedToClustering(kind1)
                             : -SortedKey::comparedToClustering(kind2);

}

bool getCompareResult(int cmp, int8_t ck_kind) {
  bool flag = false;
  switch (ck_kind) {
    case CK_KIND::INCL_END_BOUND:
      if (cmp <= 0)
        flag = true;
      break;
    case CK_KIND::EXCL_END_BOUND:
      if (cmp < 0)
        flag = true;
      break;
    case CK_KIND::INCL_START_BOUND:
      if (cmp >= 0)
        flag = true;
      break;
    case CK_KIND::EXCL_START_BOUND:
      if (cmp > 0)
        flag = true;
      break;
    default:
      break;
  }
  return flag;
}

bool
CassandraCompactionFilter::compareRangeTombstone(const char* clusterKeyData,
                                                 int32_t clusterKeySize,
                                                 int clusterKeyLength,
                                                 std::shared_ptr<rocksdb::cassandra::RangeTombstone> rangeTombstone) const {
  bool res = true;
  if (rangeTombstone->StartCkSize() > 0) {
    int cmp = compare(clusterKeyData, clusterKeySize,
                            CK_KIND::CLUSTERING,
                            clusterKeyLength,
                            rangeTombstone->StartData(),
                            rangeTombstone->StartValueSize(),
                            rangeTombstone->StartKind(),
                            rangeTombstone->StartCkSize());
    res = res && getCompareResult(cmp, rangeTombstone->StartKind());
  }

  if (rangeTombstone->EndCkSize() > 0) {
    int cmp = compare(clusterKeyData, clusterKeySize,
                            CK_KIND::CLUSTERING,
                            clusterKeyLength,
                            rangeTombstone->EndData(),
                            rangeTombstone->EndValueSize(),
                            rangeTombstone->EndKind(),
                            rangeTombstone->EndCkSize());
    res = res && getCompareResult(cmp, rangeTombstone->EndKind());
  }
  return res;
}


bool CassandraCompactionFilter::ShouldDropByMarker(const rocksdb::Slice& key,
                                                   Markers&& markers,
                                                   int64_t time_stamp,
                                                   int32_t ck_size,
                                                   size_t pkLength) const {
  Markers markers_;
  for (auto& marker : markers) {
    if (marker->MarkForDeleteAt() >= time_stamp) {
      markers_.push_back(std::move(marker));
    }
  }
  if (markers_.empty()) {
    return false;
  }

  std::map<int32_t, bool> res;
  Slice cluster_key(key.data(), key.size());
  if (partition_key_length_ > 0)
    cluster_key.remove_prefix(partition_key_length_);
  else if (pkLength > 0)
    cluster_key.remove_prefix(pkLength);
  int32_t size = 0;
  int32_t index = 0;
  for (auto& marker : markers_) {
    res[index] = compareRangeTombstone(cluster_key.data(), static_cast<int32_t>(cluster_key.size()), ck_size,marker);
    index++;
  }
  for (auto& pair : res) {
    if (pair.second) {
      size++;
    }
  }
  return res.size() > 0 && size > 0;
}


CompactionFilter::Decision CassandraCompactionFilter::FilterV2(
  int /*level*/, const Slice& key, ValueType value_type,
  const Slice& existing_value, std::string* new_value,
  std::string* /*skip_until*/) const {
  bool value_changed = false;
  RowValue row_value = RowValue::Deserialize(
    existing_value.data(), existing_value.size());

  if (ShouldDropByParitionHeader(key, row_value.LastModifiedTimePoint(),
                                 row_value.LastModifiedTime(),
                                 row_value.CkSize())) {
    return Decision::kRemove;
  }

  RowValue compacted =
    purge_ttl_on_expiration_
    ? row_value.RemoveExpiredColumns(&value_changed)
    : row_value.ConvertExpiredColumnsToTombstones(&value_changed);

  if (value_type == ValueType::kValue) {
    compacted = compacted.RemoveTombstones(gc_grace_period_);
  }

  if (value_type == ValueType::kValue &&
      compacted.TimeStamp() != NO_TIMESTAMP &&
      compacted.MarkedForDeleteAt() != NO_TIMESTAMP &&
      std::chrono::time_point<std::chrono::system_clock>(
        std::chrono::microseconds(compacted.MarkedForDeleteAt())) +
      gc_grace_period_ < std::chrono::system_clock::now()) {
    return Decision::kRemove;
  }

  if (value_changed) {
    compacted.Serialize(new_value);
    return Decision::kChangeValue;
  }
  return Decision::kKeep;
}

}  // namespace cassandra
}  // namespace rocksdb
