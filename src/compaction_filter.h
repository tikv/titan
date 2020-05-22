#pragma once

#include "db_impl.h"
#include "rocksdb/compaction_filter.h"

#include <utility>

namespace rocksdb {
namespace titandb {

class TitanCompactionFilter final : public CompactionFilter {
public:
  explicit TitanCompactionFilter(const TitanDBImpl *db,
                                 const CompactionFilter *origin, uint32_t cf_id)
      : titan_db_impl_(db), origin_filter_(origin), column_family_id_(cf_id) {}

  const char *Name() const override { return "TitanCompactionFilter"; }

  Decision FilterV2(int level, const Slice &key, ValueType value_type,
                    const Slice &value, std::string *new_value,
                    std::string *skip_until) const override {
    if (value_type != kBlobIndex) {
      return origin_filter_->FilterV2(level, key, value_type, value, new_value,
                                      skip_until);
    }

    BlobIndex blob_index;
    Slice origin_value(value.data());
    Status s = blob_index.DecodeFrom(&origin_value);
    if (!s.ok()) {
      // Unable to decode blob index. Keeping the value.
      return Decision::kKeep;
    }
    if (BlobIndex::IsDeletionMarker(blob_index)) {
      return Decision::kRemove;
    }

    BlobRecord record;
    PinnableSlice buffer;

    titan_db_impl_->mutex_.Lock();
    auto storage =
        titan_db_impl_->blob_file_set_->GetBlobStorage(column_family_id_)
            .lock();
    titan_db_impl_->mutex_.Unlock();

    if (storage) {
      ReadOptions read_options;
      s = storage->Get(read_options, blob_index, &record, &buffer);
    } else {
      // Column family not found, remove the value.
      return Decision::kRemove;
    }

    if (s.ok()) {
      bool value_changed = false;
      bool rv = origin_filter_->Filter(level, key, record.value, new_value,
                                       &value_changed);
      if (rv) {
        return Decision::kRemove;
      }
      return value_changed ? Decision::kChangeValue : Decision::kKeep;
    }

    // GetBlobRecord failed, keep the value.
    return Decision::kKeep;
  }

private:
  const TitanDBImpl *titan_db_impl_;
  const CompactionFilter *origin_filter_;
  uint32_t column_family_id_;
};

class TitanCompactionFilterFactory final : public CompactionFilterFactory {
public:
  explicit TitanCompactionFilterFactory(const TitanDBImpl *db)
      : titan_db_impl_(db), origin_filter_(nullptr),
        origin_cf_factory_(nullptr) {}

  const char *Name() const override { return "TitanCompactionFilterFactory"; }

  void SetCF(const CompactionFilter *cf) { origin_filter_ = cf; }

  void SetCFFactory(std::shared_ptr<CompactionFilterFactory> cf_factory) {
    origin_cf_factory_ = std::move(cf_factory);
  }

  std::unique_ptr<CompactionFilter>
  CreateCompactionFilter(const CompactionFilter::Context &context) override {
    const CompactionFilter *compaction_filter = origin_filter_;
    std::unique_ptr<CompactionFilter> compaction_filter_from_factory = nullptr;
    if (compaction_filter == nullptr) {
      // One of the origin_filter_ and origin_cf_factory_ must not be null
      compaction_filter_from_factory =
          origin_cf_factory_->CreateCompactionFilter(context);
      compaction_filter = compaction_filter_from_factory.get();
    }

    return std::unique_ptr<CompactionFilter>(new TitanCompactionFilter(
        titan_db_impl_, compaction_filter, context.column_family_id));
  }

private:
  const TitanDBImpl *titan_db_impl_;
  const CompactionFilter *origin_filter_;
  std::shared_ptr<CompactionFilterFactory> origin_cf_factory_;
};

} // namespace titandb
} // namespace rocksdb
