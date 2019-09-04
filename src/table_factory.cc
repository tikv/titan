#include "table_factory.h"

#include "table_builder.h"

namespace rocksdb {
namespace titandb {

Status TitanTableFactory::NewTableReader(
    const TableReaderOptions& options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* result,
    bool prefetch_index_and_filter_in_cache) const {
  return base_factory_->NewTableReader(options, std::move(file), file_size,
                                       result,
                                       prefetch_index_and_filter_in_cache);
}

TableBuilder* TitanTableFactory::NewTableBuilder(
    const TableBuilderOptions& options, uint32_t column_family_id,
    WritableFileWriter* file) const {
  std::unique_ptr<TableBuilder> base_builder(
      base_factory_->NewTableBuilder(options, column_family_id, file));
  TitanCFOptions cf_options = cf_options_;
  cf_options.blob_run_mode = blob_run_mode_.load();
  std::weak_ptr<BlobStorage> blob_storage;
  // While table factory constructed in titan for first time, db_impl_ is
  // uninitialized (nullptr), and flush may trigger. Since flush won't merge
  // blob files, we set merge level to a unreachable value.
  int num_levels = INT_MAX;
  if (db_impl_ != nullptr) {
    InstrumentedMutexLock l(db_impl_->mutex());
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(
        db_impl_->GetColumnFamilyHandle(column_family_id));
    auto cfd = cfh->cfd();
    num_levels = cfd->current()->storage_info()->num_non_empty_levels();
  }
  {
    MutexLock l(db_mutex_);
    blob_storage = vset_->GetBlobStorage(column_family_id);
  }
  return new TitanTableBuilder(
      column_family_id, db_options_, cf_options, std::move(base_builder),
      blob_manager_, blob_storage, stats_,
      std::max(1, num_levels - 2) /* merge level */, options.level);
}

std::string TitanTableFactory::GetPrintableTableOptions() const {
  assert(blob_run_mode_to_string.count(blob_run_mode_.load()) > 0);
  return base_factory_->GetPrintableTableOptions() + "  blob_run_mode: " +
         blob_run_mode_to_string.at(blob_run_mode_.load());
}

}  // namespace titandb
}  // namespace rocksdb
