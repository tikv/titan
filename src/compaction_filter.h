#pragma once

#include <string>
#include <utility>

#include "db_impl.h"
#include "rocksdb/compaction_filter.h"
#include "util/mutexlock.h"

namespace rocksdb {
namespace titandb {

class TitanCompactionFilter final : public CompactionFilter {
 public:
  TitanCompactionFilter(TitanDBImpl *db, const std::string &cf_name,
                        const CompactionFilter *original,
                        std::unique_ptr<CompactionFilter> &&owned_filter,
                        std::shared_ptr<BlobStorage> blob_storage,
                        bool skip_value)
      : db_(db),
        cf_name_(cf_name),
        blob_storage_(std::move(blob_storage)),
        original_filter_(original),
        owned_filter_(std::move(owned_filter)),
        skip_value_(skip_value),
        filter_name_(std::string("TitanCompactionfilter.")
                         .append(original_filter_->Name())) {
    assert(blob_storage_ != nullptr);
    assert(original_filter_ != nullptr);
  }

  const char *Name() const override { return filter_name_.c_str(); }

  Decision FilterV2(int level, const Slice &key, ValueType value_type,
                    const Slice &value, std::string *new_value,
                    std::string *skip_until) const override {
    if (skip_value_) {
      return original_filter_->FilterV2(level, key, value_type, Slice(),
                                        new_value, skip_until);
    }
    if (value_type != kBlobIndex) {
      return original_filter_->FilterV2(level, key, value_type, value,
                                        new_value, skip_until);
    }

    BlobIndex blob_index;
    Slice original_value(value.data());
    Status s = blob_index.DecodeFrom(&original_value);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_->db_options_.info_log,
                      "[%s] Unable to decode blob index", cf_name_.c_str());
      // TODO(yiwu): Better to fail the compaction as well, but current
      // compaction filter API doesn't support it.
      {
        MutexLock l(&db_->mutex_);
        db_->SetBGError(s);
      }
      // Unable to decode blob index. Keeping the value.
      return Decision::kKeep;
    }
    if (BlobIndex::IsDeletionMarker(blob_index)) {
      // TODO(yiwu): handle deletion marker at bottom level.
      return Decision::kKeep;
    }

    BlobRecord record;
    PinnableSlice buffer;
    ReadOptions read_options;
    s = blob_storage_->Get(read_options, blob_index, &record, &buffer);

    if (s.IsCorruption()) {
      // Could be cause by blob file beinged GC-ed, or real corruption.
      // TODO(yiwu): Tell the two cases apart.
      return Decision::kKeep;
    } else if (s.ok()) {
      auto decision = original_filter_->FilterV2(
          level, key, kValue, record.value, new_value, skip_until);

      // It would be a problem if it change the value whereas the value_type
      // is still kBlobIndex. For now, just returns kKeep.
      // TODO: we should make rocksdb Filter API support changing value_type
      // assert(decision != CompactionFilter::Decision::kChangeValue);
      if (decision == Decision::kChangeValue) {
        {
          MutexLock l(&db_->mutex_);
          db_->SetBGError(Status::NotSupported(
              "It would be a problem if it change the value whereas the "
              "value_type is still kBlobIndex."));
        }
        decision = Decision::kKeep;
      }
      return decision;
    } else {
      {
        MutexLock l(&db_->mutex_);
        db_->SetBGError(s);
      }
      // GetBlobRecord failed, keep the value.
      return Decision::kKeep;
    }
  }

 private:
  TitanDBImpl *db_;
  const std::string cf_name_;
  std::shared_ptr<BlobStorage> blob_storage_;
  const CompactionFilter *original_filter_;
  const std::unique_ptr<CompactionFilter> owned_filter_;
  bool skip_value_;
  std::string filter_name_;
};

class TitanCompactionFilterFactory final : public CompactionFilterFactory {
 public:
  TitanCompactionFilterFactory(
      const CompactionFilter *original_filter,
      std::shared_ptr<CompactionFilterFactory> original_filter_factory,
      TitanDBImpl *db, bool skip_value, const std::string &cf_name)
      : original_filter_(original_filter),
        original_filter_factory_(original_filter_factory),
        titan_db_impl_(db),
        skip_value_(skip_value),
        cf_name_(cf_name) {
    assert(original_filter != nullptr || original_filter_factory != nullptr);
    if (original_filter_ != nullptr) {
      factory_name_ = std::string("TitanCompactionFilterFactory.")
                          .append(original_filter_->Name());
    } else {
      factory_name_ = std::string("TitanCompactionFilterFactory.")
                          .append(original_filter_factory_->Name());
    }
  }

  const char *Name() const override { return factory_name_.c_str(); }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context &context) override {
    assert(original_filter_ != nullptr || original_filter_factory_ != nullptr);

    std::shared_ptr<BlobStorage> blob_storage;
    {
      MutexLock l(&titan_db_impl_->mutex_);
      blob_storage = titan_db_impl_->blob_file_set_
                         ->GetBlobStorage(context.column_family_id)
                         .lock();
    }
    if (blob_storage == nullptr) {
      assert(false);
      // Shouldn't be here, but ignore compaction filter when we hit error.
      return nullptr;
    }

    const CompactionFilter *original_filter = original_filter_;
    std::unique_ptr<CompactionFilter> original_filter_from_factory;
    if (original_filter == nullptr) {
      original_filter_from_factory =
          original_filter_factory_->CreateCompactionFilter(context);
      original_filter = original_filter_from_factory.get();
    }

    if (original_filter == nullptr) {
      return nullptr;
    }

    return std::unique_ptr<CompactionFilter>(new TitanCompactionFilter(
        titan_db_impl_, cf_name_, original_filter,
        std::move(original_filter_from_factory), blob_storage, skip_value_));
  }

 private:
  const CompactionFilter *original_filter_;
  std::shared_ptr<CompactionFilterFactory> original_filter_factory_;
  TitanDBImpl *titan_db_impl_;
  bool skip_value_;
  const std::string cf_name_;
  std::string factory_name_;
};

}  // namespace titandb
}  // namespace rocksdb
