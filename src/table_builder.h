#pragma once

#include "blob_file_builder.h"
#include "blob_file_manager.h"
#include "table/table_builder.h"
#include "titan/options.h"
#include "titan_stats.h"
#include "version_set.h"

namespace rocksdb {
namespace titandb {

class TitanTableBuilder : public TableBuilder {
 public:
  TitanTableBuilder(uint32_t cf_id, const TitanDBOptions& db_options,
                    const TitanCFOptions& cf_options,
                    std::unique_ptr<TableBuilder> base_builder,
                    std::shared_ptr<BlobFileManager> blob_manager,
                    std::weak_ptr<BlobStorage> blob_storage, TitanStats* stats,
                    int merge_level, int target_level)
      : cf_id_(cf_id),
        merge_level_(merge_level),
        target_level_(target_level),
        db_options_(db_options),
        cf_options_(cf_options),
        base_builder_(std::move(base_builder)),
        blob_manager_(blob_manager),
        blob_storage_(blob_storage),
        stats_(stats) {}

  void Add(const Slice& key, const Slice& value) override;

  Status status() const override;

  Status Finish() override;

  void Abandon() override;

  uint64_t NumEntries() const override;

  uint64_t FileSize() const override;

  bool NeedCompact() const override;

  TableProperties GetTableProperties() const override;

 private:
  bool ok() const { return status().ok(); }

  void AddBlob(const Slice& key, const Slice& value, std::string* index_value);

  bool ShouldMerge(const std::shared_ptr<BlobFileMeta>& file);

  void FinishBlob();

  friend class TableBuilderTest;

  Status status_;
  uint32_t cf_id_;
  std::unique_ptr<ColumnFamilyHandle> cf_handle_;
  int merge_level_;
  int target_level_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  std::unique_ptr<TableBuilder> base_builder_;
  std::unique_ptr<BlobFileHandle> blob_handle_;
  std::shared_ptr<BlobFileManager> blob_manager_;
  std::unique_ptr<BlobFileBuilder> blob_builder_;
  std::weak_ptr<BlobStorage> blob_storage_;

  TitanStats* stats_;
};

}  // namespace titandb
}  // namespace rocksdb
