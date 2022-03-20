#pragma once

#include <memory>

#include "blob_format.h"
#include "db/column_family.h"
#include "titan/options.h"

namespace rocksdb {
namespace titandb {

// A BlobGC encapsulates information about a blob gc.
class BlobGC {
 public:
  BlobGC(std::vector<std::shared_ptr<BlobFileMeta>>&& blob_files,
         TitanCFOptions&& _titan_cf_options, bool need_trigger_next);

  // No copying allowed
  BlobGC(const BlobGC&) = delete;
  void operator=(const BlobGC&) = delete;

  ~BlobGC();

  const std::vector<std::shared_ptr<BlobFileMeta>>& inputs() { return inputs_; }

  const TitanCFOptions& titan_cf_options() { return titan_cf_options_; }

  void SetColumnFamily(ColumnFamilyHandle* cfh);

  ColumnFamilyHandle* column_family_handle() { return cfh_; }

  ColumnFamilyData* GetColumnFamilyData();

  void MarkFilesBeingGC();

  void AddOutputFile(BlobFileMeta*);

  void ReleaseGcFiles();

  bool trigger_next() { return trigger_next_; }

 private:
  std::vector<std::shared_ptr<BlobFileMeta>> inputs_;
  std::vector<BlobFileMeta*> outputs_;
  TitanCFOptions titan_cf_options_;
  ColumnFamilyHandle* cfh_{nullptr};
  // Whether need to trigger gc after this gc or not
  const bool trigger_next_;
};

struct GCScore {
  uint64_t file_number;
  double score;
};

}  // namespace titandb
}  // namespace rocksdb
