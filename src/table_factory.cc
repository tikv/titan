#include "table_factory.h"

#include "db_impl.h"
#include "table_builder.h"

namespace rocksdb {
namespace titandb {

Status TitanTableFactory::NewTableReader(
    const ReadOptions &ro, const TableReaderOptions &options,
    std::unique_ptr<RandomAccessFileReader> &&file, uint64_t file_size,
    std::unique_ptr<TableReader> *result,
    bool prefetch_index_and_filter_in_cache) const {
  return base_factory_->NewTableReader(ro, options, std::move(file), file_size,
                                       result,
                                       prefetch_index_and_filter_in_cache);
}

TableBuilder *TitanTableFactory::NewTableBuilder(
    const TableBuilderOptions &options, WritableFileWriter *file) const {
  std::unique_ptr<TableBuilder> base_builder(
      base_factory_->NewTableBuilder(options, file));
  // When opening base DB, it may trigger flush L0. But blob_file_set_ is not
  // opened yet, then it would write to the uninitialized manifest writer. So do
  // not enable titan table builder now.
  if (!blob_file_set_->IsOpened()) {
    return base_builder.release();
  }
  TitanCFOptions cf_options = cf_options_;
  cf_options.UpdateMutableOptions(mutable_cf_options_);

  std::weak_ptr<BlobStorage> blob_storage;

  // since we force use dynamic_level_bytes=true when level_merge=true, the last
  // level of a cf is always cf_options.num_levels - 1.
  int num_levels = cf_options.num_levels;

  {
    MutexLock l(db_mutex_);
    blob_storage = blob_file_set_->GetBlobStorage(options.column_family_id);
  }

  return new TitanTableBuilder(
      options.column_family_id, db_options_, cf_options,
      std::move(base_builder), blob_manager_, blob_storage, stats_,
      std::max(1, num_levels - 2) /* merge level */, options.level_at_creation);
}

}  // namespace titandb
}  // namespace rocksdb
