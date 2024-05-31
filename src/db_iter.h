#pragma once

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <cinttypes>

#include <memory>
#include <unordered_map>

#include "db/arena_wrapped_db_iter.h"
#include "db/db_iter.h"
#include "rocksdb/env.h"

#include "blob_file_reader.h"
#include "blob_format.h"
#include "blob_storage.h"
#include "titan_logging.h"
#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

class TitanDBIterator : public Iterator {
 public:
  TitanDBIterator(const TitanReadOptions &options, BlobStorage *storage,
                  std::shared_ptr<ManagedSnapshot> snap,
                  std::unique_ptr<ArenaWrappedDBIter> iter, SystemClock *clock,
                  TitanStats *stats, Logger *info_log)
      : options_(options),
        storage_(storage),
        snap_(snap),
        iter_(std::move(iter)),
        clock_(clock),
        stats_(stats),
        info_log_(info_log) {}

  ~TitanDBIterator() {
    RecordInHistogram(statistics(stats_), TITAN_ITER_TOUCH_BLOB_FILE_COUNT,
                      files_.size());
  }

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
    type_ = TITAN_NUM_SEEK;
  }

  void SeekToLast() override {
    iter_->SeekToLast();
    type_ = TITAN_NUM_SEEK;
  }

  void Seek(const Slice &target) override {
    iter_->Seek(target);
    type_ = TITAN_NUM_SEEK;
  }

  void SeekForPrev(const Slice &target) override {
    iter_->SeekForPrev(target);
    type_ = TITAN_NUM_SEEK;
  }

  void Next() override {
    assert(Valid());
    iter_->Next();
    type_ = TITAN_NUM_NEXT;
  }

  void Prev() override {
    assert(Valid());
    iter_->Prev();
    type_ = TITAN_NUM_PREV;
  }

  Slice key() const override {
    assert(Valid());
    return iter_->key();
  }

  Slice value() const override {
    assert(Valid() && !options_.key_only);
    if (options_.key_only) return Slice();
    if (!iter_->IsBlob()) return iter_->value();

    HistogramType hist_type;
    switch (type_) {
      case TITAN_NUM_SEEK:
        hist_type = TITAN_SEEK_MICROS;
        break;
      case TITAN_NUM_NEXT:
        hist_type = TITAN_NEXT_MICROS;
        break;
      case TITAN_NUM_PREV:
        hist_type = TITAN_PREV_MICROS;
        break;
      default:
        hist_type = TITAN_SEEK_MICROS;
        assert(false);
    };
    StopWatch sw(clock_, statistics(stats_), hist_type);
    RecordTick(statistics(stats_), type_);
    status_ = GetBlobValue();
    if (!status_.ok()) {
      return Slice();
    }
    return record_.value;
  }

  bool seqno(SequenceNumber *number) const override {
    return iter_->seqno(number);
  }

 private:
  Status GetBlobValue() const {
    assert(iter_->status().ok());

    Status s;
    BlobIndex index;
    s = DecodeInto(iter_->value(), &index);
    if (!s.ok()) {
      TITAN_LOG_ERROR(
          info_log_, "Titan iterator: failed to decode blob index %s: %s",
          iter_->value().ToString(true /*hex*/).c_str(), s.ToString().c_str());
      if (options_.abort_on_failure) std::abort();
      return s;
    }

    std::string cache_key;
    auto blob_cache = storage_->blob_cache();
    if (blob_cache) {
      cache_key = storage_->EncodeBlobCache(index);
      bool cache_hit;
      s = storage_->TryGetBlobCache(cache_key, &record_, &buffer_, &cache_hit);
      if (!s.ok()) return s;
      if (cache_hit) return s;
    }

    auto it = files_.find(index.file_number);
    if (it == files_.end()) {
      std::unique_ptr<BlobFilePrefetcher> prefetcher;
      s = storage_->NewPrefetcher(index.file_number, &prefetcher);
      if (!s.ok()) {
        TITAN_LOG_ERROR(
            info_log_,
            "Titan iterator: failed to create prefetcher for blob file %" PRIu64
            ": %s",
            index.file_number, s.ToString().c_str());
        if (options_.abort_on_failure) std::abort();
        return s;
      }
      it = files_.emplace(index.file_number, std::move(prefetcher)).first;
    }

    buffer_.Reset();
    OwnedSlice blob;
    s = it->second->Get(options_, index.blob_handle, &record_, &blob);
    if (!s.ok()) {
      TITAN_LOG_ERROR(
          info_log_,
          "Titan iterator: failed to read blob value from file %" PRIu64
          ", offset %" PRIu64 ", size %" PRIu64 ": %s\n",
          index.file_number, index.blob_handle.offset, index.blob_handle.size,
          s.ToString().c_str());
      if (options_.abort_on_failure) std::abort();
    }

    if (blob_cache && options_.fill_cache) {
      Cache::Handle *cache_handle = nullptr;
      auto cache_value = new OwnedSlice(std::move(blob));
      blob_cache->Insert(cache_key, cache_value, &kBlobValueCacheItemHelper,
                         cache_value->size() + sizeof(*cache_value),
                         &cache_handle, Cache::Priority::BOTTOM);
      buffer_.PinSlice(*cache_value, UnrefCacheHandle, blob_cache,
                       cache_handle);
    } else {
      buffer_.PinSlice(blob, OwnedSlice::CleanupFunc, blob.release(), nullptr);
    }
    return s;
  }

  mutable Status status_;
  mutable BlobRecord record_;
  mutable PinnableSlice buffer_;
  TickerType type_;

  TitanReadOptions options_;
  BlobStorage *storage_;
  std::shared_ptr<ManagedSnapshot> snap_;
  std::unique_ptr<ArenaWrappedDBIter> iter_;
  mutable std::unordered_map<uint64_t, std::unique_ptr<BlobFilePrefetcher>>
      files_;

  SystemClock *clock_;
  TitanStats *stats_;
  Logger *info_log_;
};

}  // namespace titandb
}  // namespace rocksdb
