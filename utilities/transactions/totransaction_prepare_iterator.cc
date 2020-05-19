//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/totransaction_prepare_iterator.h"
#include "util/cast_util.h"
#include "util/logging.h"
#include "utilities/transactions/totransaction_db_impl.h"

#include <iostream>

namespace rocksdb {

namespace {
using ATN = TOTransactionImpl::ActiveTxnNode;
}  // namespace

Slice PrepareMapIterator::key() const {
  return Slice(pos_.data(), pos_.size());
}

const std::shared_ptr<ATN>& PrepareMapIterator::value() const { return val_; }

void PrepareMapIterator::TryPosValueToCorrectMvccVersionInLock(
    const std::list<std::shared_ptr<ATN>>& prepare_mvccs) {
  bool found = false;
  assert(!prepare_mvccs.empty());
  assert(valid_);
  for (const auto& c : prepare_mvccs) {
    if (c->prepare_ts_ <= core_->read_ts_) {
      val_ = c;
      found = true;
      break;
    }
  }
  if (!found) {
    // no satisfied mvcc version, just return arbitory one and let upper-level
    // iterator skip to next
    val_ = prepare_mvccs.back();
  }
}

void PrepareMapIterator::Next() {
  ReadLock rl(&ph_->mutex_);
  if (!valid_) {
    return;
  }
  auto it = ph_->map_.upper_bound(std::make_pair(cf_->GetID(), pos_));
  assert(it != ph_->map_.end());
  if (it->first.first != cf_->GetID()) {
    valid_ = false;
  } else {
    valid_ = true;
    pos_ = it->first.second;
    TryPosValueToCorrectMvccVersionInLock(it->second);
  }
}

void PrepareMapIterator::Prev() {
  ReadLock rl(&ph_->mutex_);
  if (!valid_) {
    return;
  }
  const auto lookup_key = std::make_pair(cf_->GetID(), pos_);
  auto it = ph_->map_.lower_bound(lookup_key);
  assert(it != ph_->map_.end() && it->first >= lookup_key);
  if (it != ph_->map_.begin()) {
    it--;
  }
  if (it->first >= lookup_key) {
    valid_ = false;
  } else {
    valid_ = true;
    pos_ = it->first.second;
    TryPosValueToCorrectMvccVersionInLock(it->second);
  }
}

void PrepareMapIterator::SeekToFirst() { Seek(""); }

void PrepareMapIterator::SeekToLast() {
  ReadLock rl(&ph_->mutex_);
  auto it = ph_->map_.lower_bound(std::make_pair(cf_->GetID() + 1, ""));
  // because prepare_heap has sentinal at the end, it's impossible to reach the
  // end
  assert(it != ph_->map_.end());
  if (it != ph_->map_.begin()) {
    it--;
  }
  if (it->first.first != cf_->GetID()) {
    valid_ = false;
  } else {
    valid_ = true;
    pos_ = it->first.second;
    TryPosValueToCorrectMvccVersionInLock(it->second);
  }
}

void PrepareMapIterator::SeekForPrev(const Slice& target) {
  ReadLock rl(&ph_->mutex_);
  const auto lookup_key =
      std::make_pair(cf_->GetID(), std::string(target.data(), target.size()));
  auto it = ph_->map_.lower_bound(lookup_key);
  // because prepare_heap_ has sentinal at the end, it's impossible to reach the
  // end
  assert(it != ph_->map_.end() && it->first >= lookup_key);
  if (it->first != lookup_key) {
    if (it != ph_->map_.begin()) {
      it--;
    }
  }

  if (it->first.first != cf_->GetID()) {
    valid_ = false;
  } else {
    assert(it->first <= lookup_key);
    valid_ = true;
    pos_ = it->first.second;
    TryPosValueToCorrectMvccVersionInLock(it->second);
  }
}

void PrepareMapIterator::Seek(const Slice& target) {
  ReadLock rl(&ph_->mutex_);
  auto it = ph_->map_.lower_bound(
      std::make_pair(cf_->GetID(), std::string(target.data(), target.size())));
  assert(it != ph_->map_.end());
  if (it->first.first != cf_->GetID()) {
    valid_ = false;
  } else {
    valid_ = true;
    pos_ = it->first.second;
    TryPosValueToCorrectMvccVersionInLock(it->second);
  }
}

PrepareMergingIterator::PrepareMergingIterator(
    std::unique_ptr<Iterator> base_iterator,
    std::unique_ptr<PrepareMapIterator> pmap_iterator)
    : forward_(true),
      current_at_base_(true),
      equal_keys_(false),
      status_(Status::OK()),
      base_iterator_(std::move(base_iterator)),
      delta_iterator_(std::move(pmap_iterator)),
      comparator_(BytewiseComparator()) {}

bool PrepareMergingIterator::BaseValid() const {
  return base_iterator_->Valid();
}

bool PrepareMergingIterator::DeltaValid() const {
  return delta_iterator_->Valid();
}

bool PrepareMergingIterator::Valid() const {
  return current_at_base_ ? BaseValid() : DeltaValid();
}

Slice PrepareMergingIterator::key() const {
  return current_at_base_ ? ExtractUserKey(base_iterator_->key())
                          : delta_iterator_->key();
}

ShadowValue PrepareMergingIterator::value() const {
  if (equal_keys_) {
    return {true /*has_prepare*/, true /*has_base*/,
            delta_iterator_->value().get(), base_iterator_->value(),
            base_iterator_->key()};
  }
  if (current_at_base_) {
    return {false /*has_prepare*/, true /*has_base*/, nullptr,
            base_iterator_->value(), base_iterator_->key()};
  }
  return {true /*has_prepare*/, false /*has_base*/,
          delta_iterator_->value().get(), Slice(), Slice()};
}

void PrepareMergingIterator::SeekToFirst() {
  forward_ = true;
  base_iterator_->SeekToFirst();
  delta_iterator_->SeekToFirst();
  UpdateCurrent();
}

void PrepareMergingIterator::SeekToLast() {
  forward_ = false;
  base_iterator_->SeekToLast();
  delta_iterator_->SeekToLast();
  UpdateCurrent();
}

void PrepareMergingIterator::Seek(const Slice& k) {
  forward_ = true;
  base_iterator_->Seek(k);
  delta_iterator_->Seek(k);
  UpdateCurrent();
}

void PrepareMergingIterator::SeekForPrev(const Slice& k) {
  forward_ = false;
  base_iterator_->SeekForPrev(k);
  delta_iterator_->SeekForPrev(k);
  UpdateCurrent();
}

void PrepareMergingIterator::Next() {
  if (!Valid()) {
    status_ = Status::NotSupported("Next() on invalid iterator");
    return;
  }

  if (!forward_) {
    // Need to change direction
    // if our direction was backward and we're not equal, we have two states:
    // * both iterators are valid: we're already in a good state (current
    // shows to smaller)
    // * only one iterator is valid: we need to advance that iterator
    forward_ = true;
    equal_keys_ = false;
    if (!BaseValid()) {
      assert(DeltaValid());
      base_iterator_->SeekToFirst();
    } else if (!DeltaValid()) {
      delta_iterator_->SeekToFirst();
    } else if (current_at_base_) {
      // Change delta from larger than base to smaller
      AdvanceDelta();
    } else {
      // Change base from larger than delta to smaller
      AdvanceBase();
    }
    if (DeltaValid() && BaseValid()) {
      if (comparator_->Equal(delta_iterator_->key(),
                             ExtractUserKey(base_iterator_->key()))) {
        equal_keys_ = true;
      }
    }
  }
  Advance();
}

void PrepareMergingIterator::Prev() {
  if (!Valid()) {
    status_ = Status::NotSupported("Prev() on invalid iterator");
    return;
  }

  if (forward_) {
    // Need to change direction
    // if our direction was backward and we're not equal, we have two states:
    // * both iterators are valid: we're already in a good state (current
    // shows to smaller)
    // * only one iterator is valid: we need to advance that iterator
    forward_ = false;
    equal_keys_ = false;
    if (!BaseValid()) {
      assert(DeltaValid());
      base_iterator_->SeekToLast();
    } else if (!DeltaValid()) {
      delta_iterator_->SeekToLast();
    } else if (current_at_base_) {
      // Change delta from less advanced than base to more advanced
      AdvanceDelta();
    } else {
      // Change base from less advanced than delta to more advanced
      AdvanceBase();
    }
    if (DeltaValid() && BaseValid()) {
      if (comparator_->Equal(delta_iterator_->key(),
                             ExtractUserKey(base_iterator_->key()))) {
        equal_keys_ = true;
      }
    }
  }

  Advance();
}

Status PrepareMergingIterator::status() const {
  if (!status_.ok()) {
    return status_;
  }
  if (!base_iterator_->status().ok()) {
    return base_iterator_->status();
  }
  return Status::OK();
}

void PrepareMergingIterator::AssertInvariants() {
  bool not_ok = false;
  if (!base_iterator_->status().ok()) {
    assert(!base_iterator_->Valid());
    not_ok = true;
  }
  if (not_ok) {
    assert(!Valid());
    assert(!status().ok());
    return;
  }

  if (!Valid()) {
    return;
  }
  if (!BaseValid()) {
    assert(!current_at_base_ && delta_iterator_->Valid());
    return;
  }
  if (!DeltaValid()) {
    assert(current_at_base_ && base_iterator_->Valid());
    return;
  }
  int compare = comparator_->Compare(delta_iterator_->key(),
                                     ExtractUserKey(base_iterator_->key()));
  (void)compare;
  if (forward_) {
    // current_at_base -> compare < 0
    assert(!current_at_base_ || compare < 0);
    // !current_at_base -> compare <= 0
    assert(current_at_base_ && compare >= 0);
  } else {
    // current_at_base -> compare > 0
    assert(!current_at_base_ || compare > 0);
    // !current_at_base -> compare <= 0
    assert(current_at_base_ && compare <= 0);
  }
  // equal_keys_ <=> compare == 0
  assert((equal_keys_ || compare != 0) && (!equal_keys_ || compare == 0));
}

void PrepareMergingIterator::Advance() {
  if (equal_keys_) {
    assert(BaseValid() && DeltaValid());
    AdvanceBase();
    AdvanceDelta();
  } else {
    if (current_at_base_) {
      assert(BaseValid());
      AdvanceBase();
    } else {
      assert(DeltaValid());
      AdvanceDelta();
    }
  }
  UpdateCurrent();
}

void PrepareMergingIterator::AdvanceDelta() {
  if (forward_) {
    delta_iterator_->Next();
  } else {
    delta_iterator_->Prev();
  }
}

void PrepareMergingIterator::AdvanceBase() {
  if (forward_) {
    base_iterator_->Next();
  } else {
    base_iterator_->Prev();
  }
}

void PrepareMergingIterator::UpdateCurrent() {
  status_ = Status::OK();
  while (true) {
    equal_keys_ = false;
    if (!BaseValid()) {
      if (!base_iterator_->status().ok()) {
        // Expose the error status and stop.
        current_at_base_ = true;
        return;
      }

      // Base has finished.
      if (!DeltaValid()) {
        // Finished
        return;
      }
      current_at_base_ = false;
      return;
    } else if (!DeltaValid()) {
      // Delta has finished.
      current_at_base_ = true;
      return;
    } else {
      int compare = (forward_ ? 1 : -1) *
                    comparator_->Compare(delta_iterator_->key(),
                                         ExtractUserKey(base_iterator_->key()));
      if (compare <= 0) {  // delta bigger or equal
        if (compare == 0) {
          equal_keys_ = true;
        }
        current_at_base_ = false;
        return;
      } else {
        current_at_base_ = true;
        return;
      }
    }
  }

  AssertInvariants();
}

PrepareFilterIterator::PrepareFilterIterator(
    DB* db, ColumnFamilyHandle* cf, TOTransactionImpl::ActiveTxnNode* core,
    std::unique_ptr<PrepareMergingIterator> input, Logger* info_log)
    : Iterator(),
      db_(db),
      cf_(cf),
      core_(core),
      input_(std::move(input)),
      valid_(false),
      forward_(true),
      info_log_(info_log) {}

bool PrepareFilterIterator::Valid() const { return valid_; }

Slice PrepareFilterIterator::key() const {
  assert(valid_);
  return key_;
}

Slice PrepareFilterIterator::value() const {
  assert(valid_);
  return val_;
}

Status PrepareFilterIterator::status() const { return status_; }

void PrepareFilterIterator::SeekToFirst() {
  forward_ = true;
  input_->SeekToFirst();
  UpdateCurrent();
}

void PrepareFilterIterator::SeekToLast() {
  forward_ = false;
  input_->SeekToLast();
  UpdateCurrent();
}

void PrepareFilterIterator::Seek(const Slice& k) {
  forward_ = true;
  input_->Seek(k);
  UpdateCurrent();
}

void PrepareFilterIterator::SeekForPrev(const Slice& k) {
  forward_ = false;
  input_->SeekForPrev(k);
  UpdateCurrent();
}

void PrepareFilterIterator::Next() {
  if (!Valid()) {
    status_ = Status::NotSupported("Next() on invalid iterator");
    return;
  }
  if (forward_ && status_.IsPrepareConflict()) {
    UpdateCurrent();
    return;
  }
  forward_ = true;
  AdvanceInputNoFilter();
  UpdateCurrent();
}

void PrepareFilterIterator::Prev() {
  if (!Valid()) {
    status_ = Status::NotSupported("Prev() on invalid iterator");
    return;
  }
  if (!forward_ && status_.IsPrepareConflict()) {
    UpdateCurrent();
    return;
  }
  forward_ = false;
  AdvanceInputNoFilter();
  UpdateCurrent();
}

void PrepareFilterIterator::AdvanceInputNoFilter() {
  if (!input_->Valid()) {
    valid_ = false;
    return;
  }
  if (forward_) {
    input_->Next();
  } else {
    input_->Prev();
  }
  valid_ = input_->Valid();
}

WriteBatchWithIndexInternal::Result PrepareFilterIterator::GetFromBatch(
    WriteBatchWithIndex* batch, const Slice& key, std::string* value) {
  Status s;
  MergeContext merge_context;
  const ImmutableDBOptions& immuable_db_options =
      static_cast_with_check<DBImpl, DB>(db_)->immutable_db_options();

  auto comparator = WriteBatchWithIndexInternal::GetComparator(batch);
  assert(comparator);
  WriteBatchWithIndexInternal::Result result =
      WriteBatchWithIndexInternal::GetFromBatch(
          immuable_db_options, batch, cf_, key, &merge_context, comparator,
          value, true /*overwrite_keys*/, &s);
  assert(s.ok());
  return result;
}

void PrepareFilterIterator::UpdateCurrent() {
  while (true) {
    if (!input_->Valid()) {
      valid_ = false;
      return;
    }
    valid_ = true;
    status_ = Status::OK();
    key_ = input_->key();
    sval_ = input_->value();
    if (!sval_.has_prepare) {
      // TODO(deyukong): eliminate copy
      val_ = std::string(sval_.base_value.data(), sval_.base_value.size());
      return;
    } else if (!sval_.has_base) {
      const auto pmap_val = sval_.prepare_value;
      auto state = pmap_val->state_.load(std::memory_order_relaxed);
      assert(state != TOTransaction::TOTransactionState::kRollback);
      if (state == TOTransaction::TOTransactionState::kCommitted) {
        auto res = GetFromBatch(&pmap_val->write_batch_, key_, &val_);
        if (res == WriteBatchWithIndexInternal::Result::kFound) {
          // if pmap_val->commit_txn_id_ <= core_->txn_id_, we should also see
          // it from lsmtree, so sval_.has_base should be true
          assert(pmap_val->commit_txn_id_ > core_->txn_id_);
        }
        assert(res == WriteBatchWithIndexInternal::Result::kFound ||
               res == WriteBatchWithIndexInternal::Result::kDeleted);
        if (pmap_val->commit_ts_ > core_->read_ts_ ||
            res == WriteBatchWithIndexInternal::Result::kDeleted) {
          AdvanceInputNoFilter();
        } else {
          return;
        }
      } else {
        assert(state == TOTransaction::TOTransactionState::kPrepared);
        if (pmap_val->prepare_ts_ > core_->read_ts_ || core_->ignore_prepare_) {
          AdvanceInputNoFilter();
        } else {
          status_ = Status::PrepareConflict(
              "conflict with an uncommitted prepare-txn");
          return;
        }
      }
    } else {
      assert(sval_.has_prepare && sval_.has_base);
      const auto pmap_val = sval_.prepare_value;
      auto state = pmap_val->state_.load(std::memory_order_relaxed);
      assert(state != TOTransaction::TOTransactionState::kRollback);
      ParsedInternalKey internalkey;
      bool parse_result =
          ParseInternalKey(sval_.base_internal_key, &internalkey);
      (void)parse_result;
      assert(parse_result);
      // a key from my own batch has timestamp == kMaxTimestamp to intend the
      // "read-own-writes" rule
      assert(internalkey.timestamp <= core_->read_ts_ ||
             internalkey.timestamp == kMaxTimeStamp);
      if (state == TOTransaction::TOTransactionState::kCommitted) {
        auto res = GetFromBatch(&pmap_val->write_batch_, key_, &val_);
        assert(res == WriteBatchWithIndexInternal::Result::kFound ||
               res == WriteBatchWithIndexInternal::Result::kDeleted);
        if (pmap_val->commit_ts_ > core_->read_ts_ ||
            pmap_val->commit_ts_ < internalkey.timestamp) {
          val_ = std::string(sval_.base_value.data(), sval_.base_value.size());
          return;
        } else if (res == WriteBatchWithIndexInternal::Result::kDeleted) {
          AdvanceInputNoFilter();
        } else {
          return;
        }
      } else {
        assert(state == TOTransaction::TOTransactionState::kPrepared);
        assert(internalkey.timestamp < pmap_val->prepare_ts_);
        if (pmap_val->prepare_ts_ > core_->read_ts_ || core_->ignore_prepare_) {
          val_ = std::string(sval_.base_value.data(), sval_.base_value.size());
          return;
        } else {
          status_ = Status::PrepareConflict("");
          return;
        }
      }
    }
  }
}

}  // namespace rocksdb
