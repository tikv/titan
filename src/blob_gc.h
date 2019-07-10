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
  BlobGC(std::vector<BlobFileMeta*>&& blob_files,
         TitanCFOptions&& _titan_cf_options, bool need_trigger_next = false);

  // No copying allowed
  BlobGC(const BlobGC&) = delete;
  void operator=(const BlobGC&) = delete;

  ~BlobGC();

  const std::vector<BlobFileMeta*>& inputs() { return inputs_; }

  void set_sampled_inputs(std::vector<BlobFileMeta*>&& files) {
    sampled_inputs_ = std::move(files);
  }

  const std::vector<BlobFileMeta*>& sampled_inputs() { return sampled_inputs_; }

  const TitanCFOptions& titan_cf_options() { return titan_cf_options_; }

  void SetColumnFamily(ColumnFamilyHandle* cfh);

  ColumnFamilyHandle* column_family_handle() { return cfh_; }

  ColumnFamilyData* GetColumnFamilyData();

  void MarkFilesBeingGC();

  void AddOutputFile(BlobFileMeta*);

  void ReleaseGcFiles();

  bool trigger_next(){
      return trigger_next_;
  }

 private:
  std::vector<BlobFileMeta*> inputs_;
  std::vector<BlobFileMeta*> sampled_inputs_;
  std::vector<BlobFileMeta*> outputs_;
  TitanCFOptions titan_cf_options_;
  ColumnFamilyHandle* cfh_{nullptr};
  const bool trigger_next_;// whether need to trigger gc after this gc or not
};

struct GCScore {
  uint64_t file_number;
  double score;
};

}  // namespace titandb
}  // namespace rocksdb
