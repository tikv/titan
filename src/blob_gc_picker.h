#pragma once

#include <memory>

#include "db/column_family.h"
#include "db/write_callback.h"
#include "file/filename.h"
#include "rocksdb/status.h"

#include "blob_file_manager.h"
#include "blob_format.h"
#include "blob_gc.h"
#include "blob_storage.h"

namespace rocksdb {
namespace titandb {

class BlobGCPicker {
 public:
  BlobGCPicker() {};
  virtual ~BlobGCPicker() {};

  // Pick candidate blob files for a new gc.
  // Returns nullptr if there is no gc to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the gc.  Caller should delete the result.
  // If allow_punch_hole is true, picker may return a punch hole gc.
  // Otherwise, picker will only return a regular gc.
  virtual std::unique_ptr<BlobGC> PickBlobGC(BlobStorage* blob_storage,
                                             bool allow_punch_hole = false) = 0;
};

class BasicBlobGCPicker final : public BlobGCPicker {
 public:
  BasicBlobGCPicker(TitanDBOptions, TitanCFOptions, TitanStats*);
  ~BasicBlobGCPicker();

  std::unique_ptr<BlobGC> PickBlobGC(BlobStorage* blob_storage,
                                     bool allow_punch_hole = false) override;

 private:
  std::unique_ptr<BlobGC> PickRegularBlobGC(BlobStorage* blob_storage);
  std::unique_ptr<BlobGC> PickPunchHoleGC(BlobStorage* blob_storage);

  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  TitanStats* stats_;

  // Check if blob_file needs to gc, return true means we need pick this
  // file for gc
  bool CheckBlobFile(BlobFileMeta* blob_file) const;
};

}  // namespace titandb
}  // namespace rocksdb
