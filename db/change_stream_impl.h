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

#pragma once
#ifndef ROCKSDB_LITE

#include <list>
#include <map>
#include <mutex>
#include <utility>
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/change_stream.h"
#include "rocksdb/db.h"
#include "util/autovector.h"

namespace rocksdb {

class VersionEdit;
class Version;

struct SimpleEvent {
  SimpleEvent(CEType t,
              uint64_t l,
              const std::vector<std::vector<std::string>>& v,
              const std::map<uint32_t, Version*>& r,
              uint32_t column_family_id,
              const std::string& cf_name,
              bool column_family_add,
              bool column_family_drop)
    :type(t),
     lsn(l),
     encoded_version(v),
     refs(r),
     column_family(column_family_id),
     column_family_name(cf_name),
     is_column_family_add(column_family_add),
     is_column_family_drop(column_family_drop){
  }
  CEType type;
  uint64_t lsn;
  std::vector<std::vector<std::string>> encoded_version;
  std::map<uint32_t, Version*> refs;
  uint32_t column_family;
  std::string column_family_name;
  bool is_column_family_add;
  bool is_column_family_drop;
};

class ChangeEventImpl: public ChangeEvent {
 public:
   ChangeEventImpl(CEType type, const std::vector<std::vector<std::string>>& ve,
                   BatchResult&& batch,
                   uint32_t column_family_id = 0,
                   const std::string& cf_name = "",
                   bool column_family_add = false,
                   bool column_family_drop = false,
                   const std::string& manifest_name = "");
   virtual ~ChangeEventImpl() = default;
   virtual CEType GetEventType() const;
   virtual const std::vector<std::vector<std::string>>& GetMetaEvent() const;
   virtual const BatchResult& GetWalEvent() const;
   virtual bool IsColumnFamilyAdd();
   virtual bool IsColumnFamilyDrop();
   virtual uint32_t GetColumnFamily();
   virtual const std::string& GetColumnFamilyName();
   virtual const std::string& GetManifestFileName();

 private:
   CEType type_;
   std::vector<std::vector<std::string>> ver_;
   BatchResult batch_;
   uint32_t column_family_;
   std::string column_family_name_;
   bool is_column_family_add_;
   bool is_column_family_drop_;
   std::string manifest_name_;
};

// ChangeStreamImpl should be created in the dbmutex to avoid version changing
// NOTE(xxxxxxxx): the possible addlock order:
// (DBImpl.mutex_)* -> mutex_ -> (inuse_mutex_)*
class ChangeStreamImpl: public ChangeStream {
 public:
   // current is {cfid -> currentVersion}, and should be refed by the called,
   // in the constructor, currentVersion will NOT be refed once more.
   ChangeStreamImpl(DB* db,
                    const std::map<uint32_t, Version*>& current,
                    std::shared_ptr<Logger> logger);

   virtual ~ChangeStreamImpl();

   void onNewLSN(uint64_t lsn);

   // NOTE(xxxxxxxx): here we removed the const qualifier because Rocksdb itself
   // does not add const qualifier on some of VersionEdit's apis
   void onNewVersion(VersionEdit& ver) ;

   void onNewVersions(const autovector<VersionEdit*>& vers);

   void onNewVersions(const autovector<autovector<VersionEdit*>>& vers);

   void onDropCFWithRefedVersion(VersionEdit& edit, Version* ver);

   void onDurableLSN(uint64_t lsn);

   size_t GetUnpublishedEventSize() const;

   void ClearInDBLock();

   virtual Status NextEvent(ChangeEvent**);

   virtual Status HeartBeat(uint64_t current_applied_lsn,
                            const std::map<uint32_t, std::set<uint64_t>>& create_lsn_hint);

   virtual uint64_t GetId() const;

   virtual std::string DebugJSON(int verbose) const;

   virtual void GetInUsedVersonForTest(std::map<uint32_t, std::list<uint64_t>>& lsns) const;

  private:
   Version* RefVersion(const VersionEdit& edit);

   // in some race situations, TransactionLogIterator will return discontinuous
   // lsns
   // we retry 3 times to avoid this. see BUG2019101801636
   Status NextWALEvent(ChangeEvent**);
   Status NextWALEventOnce(ChangeEvent**);
   void onNewVersionsInLock(std::unique_ptr<SimpleEvent>);
   void NextVersionEventInLock(const SimpleEvent& se, ChangeEvent** event);
   mutable InstrumentedMutex inuse_mutex_;
   std::map<uint32_t, std::list<std::pair<uint64_t, Version*>>> inuse_versions_;

   mutable InstrumentedMutex mutex_;
   // protected by mutex_
   std::list<std::unique_ptr<SimpleEvent>> unpublished_events_;
   std::unique_ptr<TransactionLogIterator> wal_iter_;
   uint64_t read_wal_until_lsn_;  // inclusive
   uint64_t next_read_lsn_;
   uint64_t durable_lsn_;
   DB* db_;
   std::shared_ptr<Logger> logger_;
   uint64_t id_;
   static std::atomic<uint64_t> id_gen_;
};

using ChangeStreams = std::map<uint64_t, std::unique_ptr<ChangeStreamImpl>>;
using CSP = std::pair<const uint64_t, std::unique_ptr<ChangeStreamImpl>>;

}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
