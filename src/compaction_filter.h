#pragma once

#include "db_impl.h"
#include "rocksdb/compaction_filter.h"

#include <utility>

namespace rocksdb {
namespace titandb {

class TitanCompactionFilter final : public CompactionFilter {
public:
  explicit TitanCompactionFilter(const CompactionFilter *original,
                                 std::shared_ptr<BlobStorage> blob_storage,
                                 bool manual_release)
      : blob_storage_(std::move(blob_storage)), original_filter_(original),
        manual_release_(manual_release) {}

  ~TitanCompactionFilter() override {
    if (manual_release_) {
      delete original_filter_;
    }
  }

  const char *Name() const override {
    return std::string("TitanCompactionFilter.")
        .append(original_filter_->Name())
        .c_str();
  }

  Decision FilterV2(int level, const Slice &key, ValueType value_type,
                    const Slice &value, std::string *new_value,
                    std::string *skip_until) const override {
    if (value_type != kBlobIndex) {
      return original_filter_->FilterV2(level, key, value_type, value,
                                        new_value, skip_until);
    }

    BlobIndex blob_index;
    Slice original_value(value.data());
    Status s = blob_index.DecodeFrom(&original_value);
    if (!s.ok()) {
      // Unable to decode blob index. Keeping the value.
      return Decision::kKeep;
    }
    if (BlobIndex::IsDeletionMarker(blob_index)) {
      // TODO(yiwu): handle deletion marker at bottom level.
      return Decision::kKeep;
    }

    BlobRecord record;
    PinnableSlice buffer;

    if (blob_storage_) {
      ReadOptions read_options;
      s = blob_storage_->Get(read_options, blob_index, &record, &buffer);
    } else {
      // Column family not found, remove the value.
      return Decision::kRemove;
    }

    if (s.ok()) {
      auto decision = original_filter_->FilterV2(
          level, key, ValueType::kValue, record.value, new_value, skip_until);
      // It would be a problem if it change the value whereas the value_type is
      // still kBlobIndex. For now, just returns kKeep.
      // TODO: we should make rocksdb Filter API support changing value_type
      // assert(decision != CompactionFilter::Decision::kChangeValue);
      if (decision == Decision::kChangeValue) {
        decision = Decision::kKeep;
      }
      return decision;
    }

    // GetBlobRecord failed, keep the value.
    return Decision::kKeep;
  }

private:
  std::shared_ptr<BlobStorage> blob_storage_;
  const CompactionFilter *original_filter_;
  bool manual_release_;
};

class TitanCompactionFilterFactory final : public CompactionFilterFactory {
public:
  explicit TitanCompactionFilterFactory(const TitanDBImpl *db)
      : titan_db_impl_(db), original_filter_(nullptr),
        original_filter_factory_(nullptr) {}

  const char *Name() const override {
    if (original_filter_ != nullptr) {
      return std::string("TitanCompactionFilterFactory.")
          .append(original_filter_->Name())
          .c_str();
    } else if (original_filter_factory_ != nullptr) {
      return std::string("TitanCompactionFilterFactory.")
          .append(original_filter_factory_->Name())
          .c_str();
    } else {
      return "TitanCompactionFilterFactory.unknown";
    }
  }

  void SetOriginalCompactionFilter(const CompactionFilter *cf) {
    original_filter_ = cf;
  }

  void SetOriginalCompactionFilterFactory(
      std::shared_ptr<CompactionFilterFactory> cf_factory) {
    original_filter_factory_ = std::move(cf_factory);
  }

  std::unique_ptr<CompactionFilter>
  CreateCompactionFilter(const CompactionFilter::Context &context) override {
    assert(original_filter_ != nullptr || original_filter_factory_ != nullptr);

    titan_db_impl_->mutex_.Lock();
    auto storage =
        titan_db_impl_->blob_file_set_->GetBlobStorage(context.column_family_id)
            .lock();
    titan_db_impl_->mutex_.Unlock();

    if (original_filter_ != nullptr) {
      return std::unique_ptr<CompactionFilter>(
          new TitanCompactionFilter(original_filter_, storage, false));
    }

    auto compaction_filter =
        original_filter_factory_->CreateCompactionFilter(context);
    return std::unique_ptr<CompactionFilter>(
        new TitanCompactionFilter(compaction_filter.release(), storage, true));
  }

private:
  const TitanDBImpl *titan_db_impl_;
  const CompactionFilter *original_filter_;
  std::shared_ptr<CompactionFilterFactory> original_filter_factory_;
};

} // namespace titandb
} // namespace rocksdb
