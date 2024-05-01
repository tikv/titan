#pragma once

#include <memory>

#include "db/column_family.h"

#include "blob_format.h"
#include "titan/options.h"

namespace rocksdb {
namespace titandb {

// A BlobGC encapsulates information about a blob gc.
class BlobGC {
 public:
  BlobGC(std::vector<std::shared_ptr<BlobFileMeta>>&& blob_files,
         TitanCFOptions&& _titan_cf_options, bool need_trigger_next,
         uint64_t cf_id, bool punch_hole = false);

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

  uint64_t cf_id() { return cf_id_; }

  const Snapshot* snapshot() {
    assert(use_punch_hole_);
    assert(snapshot_ != nullptr);
    return snapshot_;
  }
  void SetSnapshot(const Snapshot* snapshot) { snapshot_ = snapshot; }
  void ReleaseSnapshot(DB* db);

  bool use_punch_hole() { return use_punch_hole_; }

  bool trigger_next() { return trigger_next_; }

 private:
  std::vector<std::shared_ptr<BlobFileMeta>> inputs_;
  std::vector<BlobFileMeta*> outputs_;
  TitanCFOptions titan_cf_options_;
  const bool trigger_next_;
  uint64_t cf_id_;
  ColumnFamilyHandle* cfh_{nullptr};
  // Whether need to trigger gc after this gc or not
  bool use_punch_hole_;
  const Snapshot* snapshot_{nullptr};
};

struct GCScore {
  uint64_t file_number;
  double score;
};

}  // namespace titandb
}  // namespace rocksdb
