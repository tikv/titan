#include "db_iter.h"

namespace rocksdb {
namespace titandb {

TitanDBIterator::TitanDBIterator(const TitanReadOptions &options,
                                 BlobStorage *storage,
                                 std::shared_ptr<ManagedSnapshot> snap,
                                 std::unique_ptr<ArenaWrappedDBIter> iter,
                                 SystemClock *clock, TitanStats *stats,
                                 Logger *info_log)
    : options_(options), storage_(storage), snap_(snap), iter_(std::move(iter)),
      clock_(clock), stats_(stats), info_log_(info_log) {}

TitanDBIterator::~TitanDBIterator() {
  RecordInHistogram(statistics(stats_), TITAN_ITER_TOUCH_BLOB_FILE_COUNT,
                    files_.size());
}

bool TitanDBIterator::Valid() const { return iter_->Valid() && status_.ok(); }

Status TitanDBIterator::status() const {
  // assume volatile inner iter
  if (status_.ok()) {
    return iter_->status();
  } else {
    return status_;
  }
}

void TitanDBIterator::SeekToFirst() {
  iter_->SeekToFirst();
  if (ShouldGetBlobValue()) {
    StopWatch seek_sw(clock_, statistics(stats_), TITAN_SEEK_MICROS);
    GetBlobValue(true);
    RecordTick(statistics(stats_), TITAN_NUM_SEEK);
  }
}

void TitanDBIterator::SeekToLast() {
  iter_->SeekToLast();
  if (ShouldGetBlobValue()) {
    StopWatch seek_sw(clock_, statistics(stats_), TITAN_SEEK_MICROS);
    GetBlobValue(false);
    RecordTick(statistics(stats_), TITAN_NUM_SEEK);
  }
}

void TitanDBIterator::Seek(const Slice &target) {
  iter_->Seek(target);
  if (ShouldGetBlobValue()) {
    StopWatch seek_sw(clock_, statistics(stats_), TITAN_SEEK_MICROS);
    GetBlobValue(true);
    RecordTick(statistics(stats_), TITAN_NUM_SEEK);
  }
}

void TitanDBIterator::SeekForPrev(const Slice &target) {
  iter_->SeekForPrev(target);
  if (ShouldGetBlobValue()) {
    StopWatch seek_sw(clock_, statistics(stats_), TITAN_SEEK_MICROS);
    GetBlobValue(false);
    RecordTick(statistics(stats_), TITAN_NUM_SEEK);
  }
}

void TitanDBIterator::Next() {
  assert(Valid());
  iter_->Next();
  if (ShouldGetBlobValue()) {
    StopWatch next_sw(clock_, statistics(stats_), TITAN_NEXT_MICROS);
    GetBlobValue(true);
    RecordTick(statistics(stats_), TITAN_NUM_NEXT);
  }
}

void TitanDBIterator::Prev() {
  assert(Valid());
  iter_->Prev();
  if (ShouldGetBlobValue()) {
    StopWatch prev_sw(clock_, statistics(stats_), TITAN_PREV_MICROS);
    GetBlobValue(false);
    RecordTick(statistics(stats_), TITAN_NUM_PREV);
  }
}

Slice TitanDBIterator::key() const {
  assert(Valid());
  return iter_->key();
}

Slice TitanDBIterator::value() const {
  assert(Valid() && !options_.key_only);
  if (options_.key_only)
    return Slice();
  if (!iter_->IsBlob())
    return iter_->value();
  return record_.value;
}

bool TitanDBIterator::seqno(SequenceNumber *number) const {
  return iter_->seqno(number);
}

bool TitanDBIterator::ShouldGetBlobValue() {
  if (!iter_->Valid() || !iter_->IsBlob() || options_.key_only) {
    status_ = iter_->status();
    return false;
  }
  return true;
}

void TitanDBIterator::GetBlobValue(bool forward) {
  assert(iter_->status().ok());

  BlobIndex index;
  status_ = DecodeInto(iter_->value(), &index);
  if (!status_.ok()) {
    TITAN_LOG_ERROR(info_log_,
                    "Titan iterator: failed to decode blob index %s: %s",
                    iter_->value().ToString(true /*hex*/).c_str(),
                    status_.ToString().c_str());
    return;
  }
  while (BlobIndex::IsDeletionMarker(index)) {
    // skip deletion marker
    if (forward) {
      iter_->Next();
    } else {
      iter_->Prev();
    }
    if (!ShouldGetBlobValue()) {
      return;
    } else {
      status_ = DecodeInto(iter_->value(), &index);
      if (!status_.ok()) {
        TITAN_LOG_ERROR(info_log_,
                        "Titan iterator: failed to decode blob index %s: %s",
                        iter_->value().ToString(true /*hex*/).c_str(),
                        status_.ToString().c_str());
        return;
      }
    }
  }
  GetBlobValueImpl(index);
}

void TitanDBIterator::GetBlobValueImpl(const BlobIndex &index) {
  auto it = files_.find(index.file_number);
  if (it == files_.end()) {
    std::unique_ptr<BlobFilePrefetcher> prefetcher;
    status_ = storage_->NewPrefetcher(index.file_number, &prefetcher);
    if (!status_.ok()) {
      TITAN_LOG_ERROR(
          info_log_,
          "Titan iterator: failed to create prefetcher for blob file %" PRIu64
          ": %s",
          index.file_number, status_.ToString().c_str());
      return;
    }
    it = files_.emplace(index.file_number, std::move(prefetcher)).first;
  }

  buffer_.Reset();
  status_ = it->second->Get(options_, index.blob_handle, &record_, &buffer_);
  if (!status_.ok()) {
    TITAN_LOG_ERROR(
        info_log_,
        "Titan iterator: failed to read blob value from file %" PRIu64
        ", offset %" PRIu64 ", size %" PRIu64 ": %s\n",
        index.file_number, index.blob_handle.offset, index.blob_handle.size,
        status_.ToString().c_str());
  }
  return;
}

} // namespace titandb
} // namespace rocksdb
