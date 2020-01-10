// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif  // __STDC_FORMAT_MACROS

#include "db/change_stream_impl.h"
#include <inttypes.h>
#include <iostream>
#include <sstream>
#include "db/db_impl.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "util/cast_util.h"

namespace rocksdb {

Status ChangeEvent::CreateWalEvent(BatchResult&& batch, ChangeEvent** res) {
  *res = new ChangeEventImpl(CEType::CEWAL, {}, std::move(batch));
  return Status::OK();
}

Status ChangeEvent::CreateVersionEvent(const std::vector<std::vector<std::string>>& v,
                                       ChangeEvent** res) {
  VersionEdit ve;
  if (v.size() == 1 && v[0].size() == 1) {
    auto s = ve.DecodeFrom(v[0][0]);
    if (!s.ok()) {
      return s;
    }
    *res = new ChangeEventImpl(CEType::CEVERSION, v, BatchResult(),
                               ve.GetColumnFamily(), ve.GetColumnFamilyName(),
                               ve.IsColumnFamilyAdd(), ve.IsColumnFamilyDrop(),
                               "");
    return Status::OK();
  }
  *res = new ChangeEventImpl(CEType::CEVERSION, v, BatchResult());
  return Status::OK();
}

Status ChangeEvent::CreateAllVersionEvent(const std::string& fname, ChangeEvent** res) {
  *res = new ChangeEventImpl(CEType::CEALLVERSION,
                             {},
                             BatchResult(),
                             0,            /* cf_id */
                             "",           /* cf_name */
                             false,        /* cf_add */
                             false,        /* cf_drop */
                             fname);
  return Status::OK();
}

ChangeEventImpl::ChangeEventImpl(CEType type,
                                 const std::vector<std::vector<std::string>>& ve,
                                 BatchResult&& batch,
                                 uint32_t cf_id,
                                 const std::string& cf_name,
                                 bool column_family_add,
                                 bool column_family_drop,
                                 const std::string& manifest_name)
    :type_(type),
     ver_(ve),
     batch_(std::move(batch)),
     column_family_(cf_id),
     column_family_name_(cf_name),
     is_column_family_add_(column_family_add),
     is_column_family_drop_(column_family_drop),
     manifest_name_(manifest_name) {
}

CEType ChangeEventImpl::GetEventType() const {
  return type_;
}


const std::vector<std::vector<std::string>>& ChangeEventImpl::GetMetaEvent() const {
  return ver_;
}

const BatchResult&  ChangeEventImpl::GetWalEvent() const {
  return batch_;
}

bool ChangeEventImpl::IsColumnFamilyAdd() {
   return is_column_family_add_;
}

bool ChangeEventImpl::IsColumnFamilyDrop() {
   return is_column_family_drop_;
}

uint32_t ChangeEventImpl::GetColumnFamily() {
  return column_family_;
}

const std::string& ChangeEventImpl::GetColumnFamilyName() {
  return column_family_name_;
}

const std::string& ChangeEventImpl::GetManifestFileName() {
  return manifest_name_;
}

std::atomic<uint64_t> ChangeStreamImpl::id_gen_ = {0};

uint64_t ChangeStreamImpl::GetId() const {
  return id_;
}

ChangeStreamImpl::ChangeStreamImpl(DB* db,
                                   const std::map<uint32_t, Version*>& current,
                                   std::shared_ptr<Logger> logger)
    :ChangeStream(),
     db_(db),
     logger_(logger),
     id_(id_gen_.fetch_add(1)) {
  // TODO(xxxxxxxx): config unpublished_events_ max length
  uint64_t last_seq = db_->GetLatestSequenceNumber();
  read_wal_until_lsn_ = last_seq;
  // NOTE(xxxxxxxx): we suppose the caller has done a fsync and held X-lock
  durable_lsn_ = last_seq;
  next_read_lsn_ = read_wal_until_lsn_+1;
  // NOTE(xxxxxxxx): here DO NOT Ref versions in current again, it should be
  // Refed by the caller
  for (auto& v : current) {
    inuse_versions_[v.first].push_back({last_seq, v.second});
  }
}

ChangeStreamImpl::~ChangeStreamImpl() {
  InstrumentedMutexLock lk(&mutex_);
  InstrumentedMutexLock inuse_lk(&inuse_mutex_);
  assert(unpublished_events_.size() == 0);
  assert(inuse_versions_.size() == 0);
}


void ChangeStreamImpl::ClearInDBLock() {
  InstrumentedMutexLock lk(&mutex_);
  InstrumentedMutexLock inuse_lk(&inuse_mutex_);
  auto db_impl = static_cast_with_check<DBImpl, DB>(db_);
  for (auto& event: unpublished_events_) {
    for (auto& vpair : event->refs) {
      db_impl->UnrefVersionInDBLock(vpair.second);
    }
  }
  for (auto& vlist : inuse_versions_) {
    for (auto& vpair : vlist.second) {
      db_impl->UnrefVersionInDBLock(vpair.second);
    }
  }
  unpublished_events_.clear();
  inuse_versions_.clear();
}

Version* ChangeStreamImpl::RefVersion(const VersionEdit& edit) {
  // TODO(deyukong): assert held dbLock
  auto db_impl = static_cast_with_check<DBImpl, DB>(db_);
  return db_impl->GetAndRefVersionInDBLock(edit.GetColumnFamily());
}

void ChangeStreamImpl::onNewVersionsInLock(std::unique_ptr<SimpleEvent> event) {
  // NOTE(deyukong): here we ref Version, rather than SuperVersion
  // Superversion = Version + Memtable + Immtable, it's uncessary and grows cache
  unpublished_events_.emplace_back(std::move(event));
}

void ChangeStreamImpl::onDurableLSN(uint64_t lsn) {
  InstrumentedMutexLock lk(&mutex_);
  durable_lsn_ = std::max(durable_lsn_, lsn);
}

void ChangeStreamImpl::onNewLSN(uint64_t lsn) {
  InstrumentedMutexLock lk(&mutex_);
  if (unpublished_events_.size() == 0 || unpublished_events_.back()->type == CEType::CEVERSION) {
    std::unique_ptr<SimpleEvent> tmp;
    tmp.reset(new SimpleEvent(CEType::CEWAL, lsn, {}, {}, 0 ,"", false, false));
    unpublished_events_.emplace_back(std::move(tmp));
    return;
  }

  // NOTE(deyukong): this trick ensures the unpublished_events_ will not grow too long
  assert(unpublished_events_.back()->type == CEType::CEWAL);
  // PutLogData will make lsn == unpublished_events_.back()->lsn because PutLogData does not increse seq
  assert(lsn > unpublished_events_.back()->lsn);
  unpublished_events_.back()->lsn = lsn;
}

void ChangeStreamImpl::onDropCFWithRefedVersion(VersionEdit& edit, Version* ver) {
  InstrumentedMutexLock lk(&mutex_);
  assert(edit.has_last_seq());
  std::string encoded_ver;
  bool ok = edit.EncodeTo(&encoded_ver);
  assert(ok);
  (void)ok;
  auto tmp = std::unique_ptr<SimpleEvent>(
    new SimpleEvent(CEType::CEVERSION,
                    edit.last_seq(),
                    {{encoded_ver}},
                    {{edit.GetColumnFamily(), ver}},
                    edit.GetColumnFamily(),
                    edit.GetColumnFamilyName(),
                    false,  /* is_column_family_add */
                    true));  /* is_column_family_drop */
  onNewVersionsInLock(std::move(tmp));
}


void ChangeStreamImpl::onNewVersion(VersionEdit& edit) {
  InstrumentedMutexLock lk(&mutex_);
  assert(edit.has_last_seq());
  if (unpublished_events_.size() != 0) {
    assert(unpublished_events_.back()->lsn <= edit.last_seq());
  }
  
  std::string cf_name = edit.GetColumnFamilyName();
  uint32_t cf_id = edit.GetColumnFamily();
  bool is_cf_drop = edit.IsColumnFamilyDrop();
  bool is_cf_add = edit.IsColumnFamilyAdd();
  std::string encoded_ver;
  bool ok = edit.EncodeTo(&encoded_ver);
  assert(ok);
  (void)ok;
  std::map<uint32_t, Version*> refs;
  Version* ref = RefVersion(edit);
  refs[edit.GetColumnFamily()] = ref;
  auto tmp = std::unique_ptr<SimpleEvent>(
    new SimpleEvent(CEType::CEVERSION, edit.last_seq(), {{encoded_ver}}, refs, cf_id, cf_name , is_cf_add, is_cf_drop));
  onNewVersionsInLock(std::move(tmp));
}

void ChangeStreamImpl::onNewVersions(const autovector<VersionEdit*>& vers) {
  if (vers.size() == 0) {
    return;
  }
  InstrumentedMutexLock lk(&mutex_);
  std::vector<std::string> encoded_vers;
  std::map<uint32_t, Version*> refs;
  uint64_t seq = 0;
  for(size_t i = 0; i < vers.size(); ++i) {
    assert(vers[i]->has_last_seq());
    if (unpublished_events_.size() != 0) {
      assert(unpublished_events_.back()->lsn <= vers[i]->last_seq());
    }
    std::string encoded_ver;
    bool ok = vers[i]->EncodeTo(&encoded_ver);
    assert(ok);
    (void)ok;
    encoded_vers.emplace_back(encoded_ver);
    if (seq == 0) {
      seq = vers[i]->last_seq();
    } else {
      assert(seq == vers[i]->last_seq());
    }
    if (refs.find(vers[i]->GetColumnFamily()) == refs.end()) {
      Version* ref = RefVersion(*vers[i]);
      refs[vers[i]->GetColumnFamily()] = ref;
    }
  }
  auto tmp = std::unique_ptr<SimpleEvent>(
    new SimpleEvent(CEType::CEVERSION, seq, {encoded_vers}, refs, 0, "", false, false));
  onNewVersionsInLock(std::move(tmp));
}

void ChangeStreamImpl::onNewVersions(const autovector<autovector<VersionEdit*>>& vers) {
  if (vers.size() == 0) {
    return;
  }
  InstrumentedMutexLock lk(&mutex_);
  std::vector<std::vector<std::string>> all_encoded_vers;
  std::map<uint32_t, Version*> refs;
  uint64_t seq = 0;
  for (size_t i = 0; i < vers.size(); ++i) {
    std::vector<std::string> encoded_vers;
    for(size_t j = 0; j < vers[i].size(); ++j) {
      assert(vers[i][j]->has_last_seq());
      if (unpublished_events_.size() != 0) {
        assert(unpublished_events_.back()->lsn <= vers[i][j]->last_seq());
      }
      std::string encoded_ver;
      bool ok = vers[i][j]->EncodeTo(&encoded_ver);
      assert(ok);
      (void)ok;
      encoded_vers.emplace_back(encoded_ver);
      if (seq == 0) {
        seq = vers[i][j]->last_seq();
      } else {
        assert(seq == vers[i][j]->last_seq());
      } 
      if (refs.find(vers[i][j]->GetColumnFamily()) == refs.end()) {
        Version* ref = RefVersion(*vers[i][j]);
        refs[vers[i][j]->GetColumnFamily()] = ref;
      }
    }
    all_encoded_vers.emplace_back(encoded_vers);
  } 
  auto tmp = std::unique_ptr<SimpleEvent>(
    new SimpleEvent(CEType::CEVERSION, seq, all_encoded_vers, refs, 0, "", false, false));
  onNewVersionsInLock(std::move(tmp));
}

size_t ChangeStreamImpl::GetUnpublishedEventSize() const {
  InstrumentedMutexLock lk(&mutex_);
  return unpublished_events_.size();
}

Status ChangeStreamImpl::NextWALEvent(ChangeEvent** event) {
  static const int RETRY_TIMES = 10;
  for (int i = 0; i < RETRY_TIMES; ++i) {
    auto s = NextWALEventOnce(event);
    if (s.ok()) {
      return s;
    }
    wal_iter_.reset();
    ROCKS_LOG_WARN(logger_, "get next walevent for %d times failed:%s", i + 1,
                   s.ToString().c_str());
    sleep(2);
  }
  abort();
  // unreachable
  return Status::Corruption("get next walevent failed");
}

Status ChangeStreamImpl::NextWALEventOnce(ChangeEvent** event) {
  assert(next_read_lsn_ <= read_wal_until_lsn_);
  if (wal_iter_ == nullptr) {
    Status s = db_->GetUpdatesSince(
        static_cast<rocksdb::SequenceNumber>(next_read_lsn_),
        &wal_iter_,
        rocksdb::TransactionLogIterator::ReadOptions());
    if (!s.ok()) {
      wal_iter_.reset();
      return s;
    }
  }

  if (!wal_iter_->Valid()) {
    auto s = wal_iter_->status();
    wal_iter_.reset();
    std::stringstream ss;
    auto db_impl = static_cast_with_check<DBImpl, DB>(db_);
    ss << "read wal failed:" << s.getState()
       << ", oldestWALLsn:" << db_impl->GetOldestWalSequenceNumber()
       << ", currentLsn:" << db_impl->GetLatestSequenceNumber()
       << ", durableLsn:" << durable_lsn_;
    ROCKS_LOG_ERROR(logger_, "read wal failed:%s", ss.str().c_str());
    return s;
  }
  auto res = wal_iter_->GetBatch();
  wal_iter_->Next();
  if (res.sequence != next_read_lsn_) {
    auto db_impl = static_cast_with_check<DBImpl, DB>(db_);
    std::stringstream ss;
    ss << "next_read_lsn:" << next_read_lsn_
       << ",read_wal_until_lsn:" << read_wal_until_lsn_
       << ",oldestWALLsn:" << db_impl->GetOldestWalSequenceNumber()
       << ",currentLsn:" << db_impl->GetLatestSequenceNumber()
       << ",beginSeq:" << res.sequence << ",durableLsn:" << durable_lsn_;
    ROCKS_LOG_ERROR(logger_, "next_read_lsn err:%s", ss.str().c_str());
    return Status::Corruption("next_read_lsn not match");
  }
  next_read_lsn_ = res.sequence + WriteBatchInternal::Count(res.writeBatchPtr.get());
  // currently we can not handle PutLogData
  assert(WriteBatchInternal::Count(res.writeBatchPtr.get()));
  // NOTE(deyukong): lsn of records are continuous, but lsn of a batch are not
  // we use the assertion below to promise read_wal_until_lsn_ never sits in
  // the middle of a batch
  if (!(next_read_lsn_ <= read_wal_until_lsn_ + 1)) {
    auto db_impl = static_cast_with_check<DBImpl, DB>(db_);
    std::stringstream ss;
    ss << "next_read_lsn:" << next_read_lsn_
       << ",read_wal_until_lsn:" << read_wal_until_lsn_
       << ",oldestWALLsn:" << db_impl->GetOldestWalSequenceNumber()
       << ",currentLsn:" << db_impl->GetLatestSequenceNumber()
       << ",beginSeq:" << res.sequence << ",durableLsn:" << durable_lsn_;
    ROCKS_LOG_FATAL(logger_, "fatal err:%s", ss.str().c_str());
    abort();
  }

  assert(next_read_lsn_ <= read_wal_until_lsn_+1);
  *event = new ChangeEventImpl(CEType::CEWAL, {}, std::move(res));
  return Status::OK();
}

void ChangeStreamImpl::NextVersionEventInLock(const SimpleEvent& se, ChangeEvent** event) {
  // TODO(deyukong): assert mutex_ held
  *event = new ChangeEventImpl(CEType::CEVERSION,
                               se.encoded_version,
                               BatchResult(),
                               se.column_family,
                               se.column_family_name,
                               se.is_column_family_add,
                               se.is_column_family_drop);

  InstrumentedMutexLock lk(&inuse_mutex_);
  for (const auto& kv : se.refs) {
    inuse_versions_[kv.first].emplace_back(std::make_pair(se.lsn, kv.second));
  }
}

// the event dispatcher
Status ChangeStreamImpl::NextEvent(ChangeEvent** event) {
  if (next_read_lsn_ <= read_wal_until_lsn_) {
    return NextWALEvent(event);
  }
  {
    InstrumentedMutexLock lk(&mutex_);
    assert(next_read_lsn_ == read_wal_until_lsn_+1);
    while (unpublished_events_.size() > 0 &&
        unpublished_events_.front()->lsn < read_wal_until_lsn_) {
      // this happens only when the initial since_lsn > db_->GetLatestSequenceNumber()
      ROCKS_LOG_WARN(logger_,
        "discard invalid event lsn:%" PRIu64 ",type:%d, next_read_lsn:%" PRIu64,
        unpublished_events_.front()->lsn,
        unpublished_events_.front()->type,
        next_read_lsn_);
      unpublished_events_.pop_front();
    }
    if (unpublished_events_.size() == 0) {
      return Status::TryAgain("no newer available events");
    }
    if (unpublished_events_.front()->lsn > durable_lsn_) {
      return Status::TryAgain("new events not durable yet");
    }
    if (unpublished_events_.front()->type == CEType::CEVERSION) {
      NextVersionEventInLock(*(unpublished_events_.front().get()), event);
      unpublished_events_.pop_front();
      return Status::OK();
    }
    assert(unpublished_events_.front()->type == CEType::CEWAL);
    read_wal_until_lsn_ = unpublished_events_.front()->lsn;
    // each time we got a new wal_event, reset wal_iter_ to avoid reading til the end
    wal_iter_.reset();
    unpublished_events_.pop_front();
  }

  return NextWALEvent(event);
}

std::string ChangeStreamImpl::DebugJSON(int verbose) const {
  std::stringstream ss;
  {
    InstrumentedMutexLock lk(&mutex_);
    ss << "{\"unpublished_events\":[";
    bool first = true;
    for (auto& e : unpublished_events_) {
      if (first) {
        ss << "{";
        first = false;
      } else {
        ss << ",{";
      }
      ss << "\"type\":" << static_cast<int32_t>(e->type);
      ss << ",\"last_seq\":" << e->lsn;
      if (e->type != CEType::CEWAL) {
        ss << ",\"cfid\":" << e->column_family;
        ss << ",\"cfname\":"
           << "\"" << e->column_family_name << "\"";
        ss << ",\"isadd\":" << e->is_column_family_add;
        ss << ",\"isdrop\":" << e->is_column_family_drop;
        if (verbose == 0) {
          ss << ",\"refs\":[";
          bool ifirst = true;
          for (auto& ref : e->refs) {
            if (ifirst) {
              ss << "{";
              ifirst = false;
            } else {
              ss << ",{";
            }
            ss << "\"cfid\":" << ref.first;
            ss << ",\"ver_num\":" << ref.second->GetVersionNumber();
            ss << "}";
          }
          ss << "]";
        }
      }
      ss << "}";
    }
    ss << "]";
  }

  {
    InstrumentedMutexLock lk(&inuse_mutex_);
    ss << ",\"inuse_versions\":[";
    bool first = true;
    for (auto& elist : inuse_versions_) {
      if (first) {
        ss << "{";
        first = false;
      } else {
        ss << ",{";
      }
      ss << "\"cfid\":" << elist.first;
      ss << ",\"versions\":[";
      bool ifirst = true;
      for (auto& e : elist.second) {
        if (ifirst) {
          ss << "{";
          ifirst = false;
        } else {
          ss << ",{";
        }
        ss << "\"last_seq\":" << e.first;
        ss << ",\"ver_num\":" << e.second->GetVersionNumber();
        ss << "}";
      }
      ss << "]";
      ss << "}";  // each inuse_element
    }
    ss << "]";  // inuse_versions
  }
  ss << "}";
  return ss.str();
}

Status ChangeStreamImpl::HeartBeat(uint64_t current_applied_lsn,
                                   const std::map<uint32_t, std::set<uint64_t>>& create_lsn_hint) {
  std::vector<Version*> to_unrefs;
  {
    InstrumentedMutexLock lk(&inuse_mutex_);
    for (const auto& kv : create_lsn_hint) {
      if (inuse_versions_.find(kv.first) == inuse_versions_.end()) {
        ROCKS_LOG_WARN(logger_, "heartbeat cfid:%u not found, maybe exist before reloadReadReplica",
                                kv.first);
        continue;
      }
      if (kv.second.size() == 0) {
        return Status::InvalidArgument("cf version zero");
      }
    }
    std::map<uint32_t, std::set<uint64_t>> my_create_lsns;
    for (auto& vlist : inuse_versions_) {
      my_create_lsns[vlist.first] = {};
      for (auto& ver : vlist.second) {
        my_create_lsns[vlist.first].insert(ver.first);
      }
    }
    for (auto& vlist : inuse_versions_) {
      if (create_lsn_hint.find(vlist.first) == create_lsn_hint.end()) {
        // slave dont have this cf
        while (vlist.second.size() > 0 &&
               vlist.second.front().first < current_applied_lsn) {
          ROCKS_LOG_WARN(logger_, "compare remove version: lsn:%" PRIu64
                                  ",remote_current_applied_lsn:%" PRIu64
                                  ",version_id:%" PRIu64 ",cfid:%u",
                         vlist.second.front().first,
                         current_applied_lsn,
                         vlist.second.front().second->GetVersionNumber(),
                         vlist.first);
          to_unrefs.emplace_back(vlist.second.front().second);
          vlist.second.pop_front();
        }
      } else {
        // erase all versions until slave's first active version
        // slave's active version are ordered by creation-lsn, we
        // erase until slave's first active lsn for simple
        assert(create_lsn_hint.at(vlist.first).size() > 0);
        uint64_t slave_min_create_lsn =
            *create_lsn_hint.at(vlist.first).cbegin();
        while (vlist.second.size() > 0 &&
               vlist.second.front().first < slave_min_create_lsn) {
          ROCKS_LOG_WARN(logger_, "compare remove version: lsn:%" PRIu64
                                  ",remote_current_applied_lsn:%" PRIu64
                                  ",version_id:%" PRIu64 ",cfid:%u",
                         vlist.second.front().first,
                         current_applied_lsn,
                         vlist.second.front().second->GetVersionNumber(),
                         vlist.first);
          to_unrefs.emplace_back(vlist.second.front().second);
          vlist.second.pop_front();
        }
      }
      auto it = inuse_versions_.begin();
      while (it != inuse_versions_.end()) {
        if (it->second.size() == 0) {
          it = inuse_versions_.erase(it);
        } else {
          it++;
        }
      }
    }
  }

  // see the comment in change_stream_impl.h
  // take care of the lock order
  if (to_unrefs.size()) {
    auto db_impl = static_cast_with_check<DBImpl, DB>(db_);
    for (auto& e : to_unrefs) {
      db_impl->UnrefVersion(e);
    }
  }
  return Status::OK();
}

void ChangeStreamImpl::GetInUsedVersonForTest(std::map<uint32_t, std::list<uint64_t>>& lsns) const {
  for (auto& vlist : inuse_versions_) {
    lsns[vlist.first] = {};
    for (auto& ver : vlist.second) {
      lsns[vlist.first].push_back(ver.first);
    }
  }
  return;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
