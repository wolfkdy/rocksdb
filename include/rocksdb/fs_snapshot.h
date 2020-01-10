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

#include <iostream>
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace rocksdb {

class DB;

struct FileSlice {
  std::string fname;
  size_t file_size;
  size_t offset;
  Slice data;
};

/*
 * example
 * for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
 *   const auto& slice = iter->Current();
 *   std::string filename = slice.fname;
 *   std::vector<char> buf = slice.data;
 *   ssize_t offset = slice.offset;
 *   M.respond(filename, buf, offset);
 * }
 */
class FsSnapshotIterator {
 public:
  FsSnapshotIterator() {}
  virtual ~FsSnapshotIterator() {}

  virtual void SeekToFirst() = 0;

  virtual bool Valid() const = 0;

  virtual const FileSlice& Current() const = 0;

  virtual void Next() = 0;

  virtual Status status() const = 0;
};

class FsSnapshotRandomReader {
 public:
  FsSnapshotRandomReader() {}
  virtual ~FsSnapshotRandomReader() {}
  virtual Status Read(const std::string& name, uint64_t offset, std::vector<char>*) = 0;
};

/*
 * example
 * FsSnapshot* snapshot = nullptr;
 * FsSnapshotIterator* it = nullptr;
 * FsSnapshot::Create(_db, &snapshot);
 * v->CreateIterator(1024*1024, &it);
 */
class FsSnapshot {
 public:
  // NOTE(xxxxxxxx): application-level MUST guarantees that
  // no more inserts are in parallel with FsSnapshot::Create.
  // Because we dont copy wal in this api, If this restriction
  // is violated, data consistency will not be guaranteed.
  static Status Create(DB* db, FsSnapshot** snapshot_ptr);

  virtual Status CreateIterator(size_t chunk_size, FsSnapshotIterator**) = 0;

  virtual Status CreateReader(size_t chunk_size, FsSnapshotRandomReader**) = 0;

  virtual Status GetSnapshotFiles(std::map<std::string, uint64_t>* flist) = 0;

  virtual Status CopyManifest(const std::string& dest) = 0;

  virtual ~FsSnapshot() {}
};

}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
