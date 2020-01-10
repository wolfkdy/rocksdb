//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef ROCKSDB_LITE

#include <iostream>
#include "common/common.h"
#include "utilities/fs_snapshot/fs_snapshot_impl.h"
#include "util/filename.h"
#include "util/file_util.h"
namespace rocksdb {

FsSnapshotRandomReaderImpl::FsSnapshotRandomReaderImpl(size_t chunk_size,
         const std::map<std::string, uint64_t>& files,
         const std::string& current_fname,
         const std::string& manifest_fname,
         const std::string& db_name,
         Env* env)
    :chunk_size_(chunk_size),
     files_(files),
     current_fname_(current_fname),
     manifest_fname_(manifest_fname),
     db_name_(db_name),
     env_(env) {
}

Status FsSnapshotRandomReaderImpl::Read(
        const std::string& name, uint64_t offset, std::vector<char>* slice) {
  if (name == current_fname_) {
    if (offset != 0) {
      return Status::InvalidArgument("current_file should read from offest:0");
    }
    assert(files_.find(name) != files_.end());
    std::string content = manifest_fname_.substr(1) + "\n";
    assert(files_[name] == content.size());
    slice->resize(content.size());
    char *p = slice->data();
    CommonMemCopy(p, content.size(), content.c_str(), content.size());
    return Status::OK();
  }
  uint64_t size = 0;
  auto it = files_.find(name);
  if (it == files_.end()) {
    return Status::InvalidArgument("fname not found");
  }
  size = it->second;
  if (offset >= size) {
    return Status::InvalidArgument("invalid offset");
  }
  std::unique_ptr<RandomAccessFile> file;
  const EnvOptions soptions;
  auto rela_path = db_name_ + name;
  Status s = env_->NewRandomAccessFile(rela_path, &file, soptions);
  if (!s.ok()) {
    return s;
  }
  size_t chunk_size = std::min(size - offset, chunk_size_);
  slice->resize(chunk_size);
  Slice tmp;
  return file->Read(offset, chunk_size, &tmp, slice->data());
}

FsSnapshotIteratorImpl::FsSnapshotIteratorImpl(
        size_t chunk_size, const std::map<std::string, uint64_t>& files,
        const std::string& current_fname, const std::string& manifest_fname,
        const std::string& db_name, Env* env)
    :chunk_size_(chunk_size),
     status_(Status::OK()),
     valid_(false),
     files_(files),
     current_fname_(current_fname),
     manifest_fname_(manifest_fname),
     slice_(nullptr),
     curr_file_(nullptr),
     db_name_(db_name),
     env_(env) {
}

void FsSnapshotIteratorImpl::PositionAtCrurentFile() {
  assert(slice_ == nullptr);
  assert(valid_);
  assert(status_.ok());
  std::string content = manifest_fname_.substr(1) + "\n";
  CommonMemCopy(buf_, content.size(), content.c_str(), content.size());
  slice_.reset(new FileSlice{current_fname_, content.size(), 0, Slice(buf_, content.size())});
}

void FsSnapshotIteratorImpl::PositionAtFileFront(const std::string& name) {
  assert(slice_ == nullptr);
  assert(curr_file_ == nullptr);
  assert(files_.find(name) != files_.end());
  if (name == current_fname_) {
    PositionAtCrurentFile();
    return;
  }
  std::unique_ptr<SequentialFile> file;
  const EnvOptions soptions;
  const std::string rela_path = db_name_ + name;
  auto s = env_->NewSequentialFile(rela_path, &file, soptions);
  if (!s.ok()) {
    valid_ = false;
    status_ = s;
    return;
  }
  curr_file_.reset(new SequentialFileReader(std::move(file), rela_path));
  uint64_t file_size = files_[name];
  // TODO(xxxxxxxx): hook and test manifest filesize changes
  slice_.reset(new FileSlice{name, file_size, 0, Slice()});
  size_t chunk_size = std::min(chunk_size_, file_size);
  s = curr_file_->Read(chunk_size, &(slice_->data), buf_);
  if (!s.ok()) {
    valid_ = false;
    status_ = s;
    return;
  }
  assert(valid_);
  assert(status_.ok());
}

void FsSnapshotIteratorImpl::SeekToFirst() {
  if (files_.size() == 0) {
    // valid_ = false;
    // status_ = Status::OK();
    return;
  }
  valid_ = true;
  PositionAtFileFront(files_.begin()->first);
}

bool FsSnapshotIteratorImpl::Valid() const {
  return valid_;
}

const FileSlice& FsSnapshotIteratorImpl::Current() const {
  assert(slice_ != nullptr);
  return *slice_.get();
}

void FsSnapshotIteratorImpl::Next() {
  assert(slice_ != nullptr);
  assert(slice_->data.size() + slice_->offset <= slice_->file_size);
  if (slice_->data.size() + slice_->offset == slice_->file_size) {
    auto it = files_.find(slice_->fname);
    assert(it != files_.end());
    it++;
    if (it == files_.end()) {
      // to the end
      valid_ = false;
      return;
    }
    curr_file_ = nullptr;
    slice_ = nullptr;
    PositionAtFileFront(it->first);
    return;
  }
  assert(curr_file_ != nullptr);
  slice_->offset += slice_->data.size();
  size_t chunk_size = std::min(chunk_size_,
        slice_->file_size - slice_->offset - slice_->data.size());
  auto s = curr_file_->Read(chunk_size, &(slice_->data), buf_);
  assert(chunk_size >= slice_->data.size());
  //std::cout << slice_->data.size() << std::endl;
  if (!s.ok()) {
    valid_ = false;
    status_ = s;
    return;
  }
  assert(valid_);
  assert(status_.ok());
}

Status FsSnapshot::Create(DB* db, FsSnapshot** snapshot_ptr) {
  auto tmp = new FsSnapshotImpl(db);
  auto s = tmp->Init();
  if (!s.ok()) {
    return s;
  }
  *snapshot_ptr = tmp;
  return Status::OK();
}

FsSnapshotImpl::FsSnapshotImpl(DB* db)
    :file_del_disabled_(false),
     current_fname_(""),
     manifest_fname_(""),
     db_(db) {
}

Status FsSnapshotImpl::Init() {
  // TODO(xxxxxxxx): status
  db_->DisableFileDeletions();
  file_del_disabled_ = true;
  std::vector<std::string> files;
  uint64_t manifest_file_size = 0;
  auto s = db_->GetLiveFiles(files, &manifest_file_size, true /* flush_memtable */);
  if (!s.ok()) {
    return s;
  }

  /* sanity check */
  for (auto& v : files) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(v, &number, &type);
    if (!ok) {
      return Status::Corruption("Can't parse file name. This is very bad");
    }
    size_t fsize = 0;
    if (type == kCurrentFile) {
      current_fname_ = v;
      // NOTE(xxxxxxxx): fill it later
    } else if (type == kDescriptorFile) {
      // NOTE(xxxxxxxx): we should have only 1 manifest file from GetLiveFiles
      if (manifest_fname_ != "") {
        std::stringstream ss;
        ss << "two manifest files:" << v << "," << manifest_fname_;
        return Status::Corruption(ss.str());
      }
      manifest_fname_ = v;
      fsize = manifest_file_size;
    } else {
      auto rela_path = db_->GetName() + v;
      s = db_->GetEnv()->GetFileSize(rela_path, &fsize);
      if (!s.ok()) {
        return s;
      }
    }
    files_[v] = fsize;
  }
  assert(manifest_fname_.size() > 0 && manifest_fname_[0] == '/');
  files_[current_fname_] = manifest_fname_.size(); /* - '/', + '\n' */
  assert(files_.size() == files.size());
  if (manifest_fname_ == "" || current_fname_ == "") {
    return Status::Corruption("no manifest or current");
  }
  return s;
}

FsSnapshotImpl::~FsSnapshotImpl() {
  if (file_del_disabled_) {
    db_->EnableFileDeletions(false);
  }
}

Status FsSnapshotImpl::CreateIterator(size_t chunk_size, FsSnapshotIterator** p) {
  if (chunk_size < 100*1024 || chunk_size > FsSnapshotIteratorImpl::MAX_SLICE_SIZE) {
    return Status::InvalidArgument("invalid chunk_size");
  }
  *p = new FsSnapshotIteratorImpl(chunk_size,
                                  files_,
                                  current_fname_,
                                  manifest_fname_,
                                  db_->GetName(),
                                  db_->GetEnv());
  return Status::OK();
}

Status FsSnapshotImpl::CreateReader(size_t chunk_size, FsSnapshotRandomReader** p) {
  if (chunk_size < 100*1024 || chunk_size > FsSnapshotIteratorImpl::MAX_SLICE_SIZE) {
    return Status::InvalidArgument("invalid chunk_size");
  }
  *p = new FsSnapshotRandomReaderImpl(chunk_size,
                                      files_,
                                      current_fname_,
                                      manifest_fname_,
                                      db_->GetName(),
                                      db_->GetEnv());
  return Status::OK();
}

Status FsSnapshotImpl::GetSnapshotFiles(std::map<std::string, uint64_t>* flist) {
  *flist = files_;
  return Status::OK();
}

Status FsSnapshotImpl::CopyManifest(const std::string& dest) {
  const std::string rela_path = db_->GetName() + manifest_fname_;
  assert(files_.find(manifest_fname_) != files_.end());
  return CopyFile(db_->GetEnv(), rela_path, db_->GetName() + "/" + dest, files_[manifest_fname_], true/* use_fsync */);
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
