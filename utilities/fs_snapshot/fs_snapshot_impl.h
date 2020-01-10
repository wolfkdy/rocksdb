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

#include <set>
#include "rocksdb/db.h"
#include "rocksdb/fs_snapshot.h"
#include "util/file_reader_writer.h"
namespace rocksdb {

class FsSnapshotRandomReaderImpl: public FsSnapshotRandomReader {
 public:
  FsSnapshotRandomReaderImpl(size_t chunk_size,
                         const std::map<std::string, uint64_t>& files,
                         const std::string& current_fname,
                         const std::string& manifest_fname,
                         const std::string& db_name,
                         Env* env);
  virtual ~FsSnapshotRandomReaderImpl() {}
  virtual Status Read(const std::string& name, uint64_t offset, std::vector<char>*);

 private:
  size_t chunk_size_;
  std::map<std::string, uint64_t> files_;
  const std::string current_fname_;
  const std::string manifest_fname_;
  const std::string db_name_;
  Env* env_;
};

class FsSnapshotIteratorImpl: public FsSnapshotIterator {
 public:
  FsSnapshotIteratorImpl(size_t chunk_size,
                         const std::map<std::string, uint64_t>& files,
                         const std::string& current_fname,
                         const std::string& manifest_fname,
                         const std::string& db_name,
                         Env* env);
  virtual ~FsSnapshotIteratorImpl() {}
  virtual void SeekToFirst();
  virtual bool Valid() const;
  virtual const FileSlice& Current() const;
  virtual void Next();
  virtual Status status() const { return status_; }

  static constexpr size_t MAX_SLICE_SIZE = 4*1024*1024;

 private:
  void PositionAtCrurentFile();
  void PositionAtFileFront(const std::string& name);

  size_t chunk_size_;
  Status status_;
  bool valid_;
  std::map<std::string, uint64_t> files_;
  const std::string current_fname_;
  const std::string manifest_fname_;
  std::unique_ptr<FileSlice> slice_;
  std::unique_ptr<SequentialFileReader> curr_file_;
  char buf_[MAX_SLICE_SIZE];
  const std::string db_name_;
  Env* env_;
};

class FsSnapshotImpl: public FsSnapshot {
 public:
  // step 0: disableFileDeletion
  // step 1: flush memtable
  // step 2: getAliveFiles into files_
  FsSnapshotImpl(DB *db);

  Status Init();
  virtual Status CreateIterator(size_t chunk_size, FsSnapshotIterator**);
  virtual Status CreateReader(size_t chunk_size, FsSnapshotRandomReader**);
  virtual Status GetSnapshotFiles(std::map<std::string, uint64_t>* flist);
  virtual Status CopyManifest(const std::string& dest);
  // step 0: enableFileDeletion
  virtual ~FsSnapshotImpl();

 private:
  bool file_del_disabled_;
  std::map<std::string, uint64_t> files_;
  std::string current_fname_;
  std::string manifest_fname_;
  DB* db_;
};

}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
