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

#include <functional>
#include <map>
#include <set>
#include <list>
#include <string>
#include <vector>
#include "rocksdb/options.h"
#include "rocksdb/transaction_log.h"


namespace rocksdb {

enum class CEType : std::uint32_t {
  // version edit change
  CEVERSION,
  // wal change
  CEWAL,
  // all version, currently not used
  CEALLVERSION,
};

using EventCFOptionFn =
    std::function<ColumnFamilyOptions(const std::string& cfname)>;

class ChangeEvent {
 public:
   ChangeEvent() = default;
   virtual ~ChangeEvent() = default;
   virtual CEType GetEventType() const = 0;
   virtual const std::vector<std::vector<std::string>>& GetMetaEvent() const = 0;
   virtual const BatchResult& GetWalEvent() const = 0;
   static Status CreateWalEvent(BatchResult&&, ChangeEvent**);
   static Status CreateVersionEvent(const std::vector<std::vector<std::string>>&, ChangeEvent**);
   static Status CreateAllVersionEvent(const std::string& manifest_file, ChangeEvent**);
   virtual bool IsColumnFamilyAdd() = 0;
   virtual bool IsColumnFamilyDrop() = 0;
   virtual uint32_t GetColumnFamily() = 0;
   virtual const std::string& GetColumnFamilyName() = 0;
   virtual const std::string& GetManifestFileName() = 0;
};

class ChangeStream {
 public:
   ChangeStream() = default;
   virtual ~ChangeStream() = default;
   virtual Status NextEvent(ChangeEvent**) = 0;
   virtual Status HeartBeat(uint64_t current_applied_lsn,
                            // cfId -> versionIds
                            const std::map<uint32_t, std::set<uint64_t>>& create_lsn_hint) = 0;
   virtual uint64_t GetId() const = 0;
   virtual std::string DebugJSON(int verbose) const = 0;
   virtual void GetInUsedVersonForTest(std::map<uint32_t, std::list<uint64_t>>& lsns) const = 0;
   enum class ManifestType : std::uint32_t {
     MANIFEST_FILE,
     MANIFEST_SNAPSHOT,
   };
};
}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
