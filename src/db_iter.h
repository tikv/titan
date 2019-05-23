#pragma once

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "db/db_iter.h"
#include "rocksdb/statistics.h"

namespace rocksdb {
namespace titandb {

class TitanDBIterator : public Iterator {
 public:
  TitanDBIterator(const ReadOptions& options, BlobStorage* storage,
                  std::shared_ptr<ManagedSnapshot> snap,
                  std::unique_ptr<ArenaWrappedDBIter> iter, Env* env,
                  Statistics* stats)
      : options_(options),
        storage_(storage),
        snap_(snap),
        iter_(std::move(iter)),
        env_(env),
        stats_(stats) {}

  bool Valid() const override { return iter_->Valid() && status_.ok(); }

  Status status() const override {
    // assume volatile inner iter
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void SeekToFirst() override {
    iter_->SeekToFirst();
    if (Check()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void SeekToLast() override {
    iter_->SeekToLast();
    if (Check()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void Seek(const Slice& target) override {
    iter_->Seek(target);
    if (Check()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void SeekForPrev(const Slice& target) override {
    iter_->SeekForPrev(target);
    if (Check()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void Next() override {
    assert(Valid());
    iter_->Next();
    if (Check()) {
      StopWatch next_sw(env_, stats_, BLOB_DB_NEXT_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_NEXT);
    }
  }

  void Prev() override {
    assert(Valid());
    iter_->Prev();
    if (Check()) {
      StopWatch prev_sw(env_, stats_, BLOB_DB_PREV_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_PREV);
    }
  }

  Slice key() const override {
    assert(Valid());
    return iter_->key();
  }

  Slice value() const override {
    assert(Valid());
    if (!iter_->IsBlob()) return iter_->value();
    return record_.value;
  }

 private:
  bool Check() {
    if (!iter_->Valid() || !iter_->IsBlob()) {
      status_ = iter_->status();
      return false;
    }
    return true;
  }

  void GetBlobValue() {
    assert(iter_->status().ok());

    BlobIndex index;
    status_ = DecodeInto(iter_->value(), &index);
    if (!status_.ok()) {
      fprintf(stderr, "GetBlobValue decode blob index err:%s\n",
              status_.ToString().c_str());
      abort();
    }

    auto it = files_.find(index.file_number);
    if (it == files_.end()) {
      std::unique_ptr<BlobFilePrefetcher> prefetcher;
      status_ = storage_->NewPrefetcher(index.file_number, &prefetcher);
      if (status_.IsCorruption()) {
        fprintf(stderr,
                "key:%s GetBlobValue err:%s with sequence number:%" PRIu64 "\n",
                iter_->key().ToString(true).c_str(), status_.ToString().c_str(),
                options_.snapshot->GetSequenceNumber());
      }
      if (!status_.ok()) return;
      it = files_.emplace(index.file_number, std::move(prefetcher)).first;
    }

    buffer_.Reset();
    status_ = it->second->Get(options_, index.blob_handle, &record_, &buffer_);
    return;
  }

  Status status_;
  BlobRecord record_;
  PinnableSlice buffer_;

  ReadOptions options_;
  BlobStorage* storage_;
  std::shared_ptr<ManagedSnapshot> snap_;
  std::unique_ptr<ArenaWrappedDBIter> iter_;
  std::map<uint64_t, std::unique_ptr<BlobFilePrefetcher>> files_;

  Env* env_;
  Statistics* stats_;
};

}  // namespace titandb
}  // namespace rocksdb
