//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Syncpoint prevents us building and running tests in release
#ifndef ROCKSDB_LITE

#ifndef OS_WIN
#include <unistd.h>
#endif
#include <iostream>
#include <thread>
#include <utility>
#include <iostream>
#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "utilities/fs_snapshot/fs_snapshot_impl.h"
#include "util/fault_injection_test_env.h"
#include "util/sync_point.h"
#include "util/testharness.h"


namespace rocksdb {

class FsSnapshotTest: public DBTestBase {
 public:
  FsSnapshotTest(): DBTestBase("/fs_snapshot_test") {}
};

TEST_F(FsSnapshotTest, CompareCountAndSize) {
  std::unique_ptr<MockEnv> env{new MockEnv(Env::Default())};
  Options options;
  options.create_if_missing = true;
  options.env = env.get();
  DB* db;

  ASSERT_OK(DB::Open(options, "/dir/db", &db));
  Random rnd(301);
  for (size_t i = 0; i < 1000000; ++i) {
    db->Put(WriteOptions(), RandomString(&rnd, 10), RandomString(&rnd, 10));
  }

  uint64_t manifest_size = 0;
  std::vector<std::string> files;
  std::map<std::string, std::pair<size_t, size_t>> file_size_map;
  db->DisableFileDeletions();
  db->GetLiveFiles(files, &manifest_size);
  db->EnableFileDeletions(false);

  FsSnapshot* fs_snapshot = nullptr;
  FsSnapshotIterator* fs_snapshot_iter = nullptr;
  FsSnapshotRandomReader* fs_snapshot_reader = nullptr;
  ASSERT_OK(FsSnapshot::Create(db, &fs_snapshot));
  ASSERT_OK(fs_snapshot->CreateIterator(1024*1024, &fs_snapshot_iter));
  ASSERT_OK(fs_snapshot->CreateReader(1024*1024, &fs_snapshot_reader));
  for (fs_snapshot_iter->SeekToFirst(); fs_snapshot_iter->Valid(); fs_snapshot_iter->Next()) {
    auto& curr = fs_snapshot_iter->Current();
    if (file_size_map.find(curr.fname) == file_size_map.end()) {
      file_size_map[curr.fname] = {curr.file_size, 0};
    }
    ASSERT_EQ(file_size_map[curr.fname].second, curr.offset);
    file_size_map[curr.fname].second += curr.data.size();
    std::vector<char> buf;
    // std::cout << curr.fname << ' ' << curr.offset << ' ' << curr.file_size << std::endl;
    ASSERT_OK(fs_snapshot_reader->Read(curr.fname, curr.offset, &buf));
    ASSERT_TRUE(buf.size() >= curr.data.size());
    ASSERT_EQ(0, memcmp(buf.data(), curr.data.data(), curr.data.size()));
  }
  ASSERT_OK(fs_snapshot_iter->status());
  ASSERT_EQ(file_size_map.size(), files.size());
  for (auto& v : files) {
    uint64_t file_size = 0;
    ASSERT_OK(env->GetFileSize(db->GetName() + v, &file_size));
    ASSERT_EQ(file_size, file_size_map[v].second);
  }  
  delete fs_snapshot;
  delete fs_snapshot_iter;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as fs_snapshot_impl is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
