#pragma once

#include <memory>

#include "rocksdb/status.h"

#include "blob_file_manager.h"
#include "blob_format.h"
#include "blob_gc.h"

namespace rocksdb {
namespace titandb {

class PunchHoleGCJob {
 public:
  PunchHoleGCJob(uint32_t cf_id, std::unique_ptr<BlobGC> blob_gc,
                 DBImpl* base_db_impl, const TitanDBOptions& db_options,
                 Env* env, const EnvOptions& env_options,
                 const Snapshot* snapshot, std::atomic_bool* shuting_down)
      : cf_id_(cf_id),
        blob_gc_(std::move(blob_gc)),
        base_db_impl_(base_db_impl),
        db_options_(db_options),
        env_(env),
        env_options_(env_options),
        snapshot_(snapshot),
        shuting_down_(shuting_down) {}
  PunchHoleGCJob(const PunchHoleGCJob&) = delete;
  void operator=(const PunchHoleGCJob&) = delete;
  ~PunchHoleGCJob() { Cleanup(); };

  Status Run();
  // REQUIRE: db mutex held
  void Finish() { UpdateBlobFilesMeta(); };

  uint32_t cf_id() const { return cf_id_; }
  const Snapshot* snapshot() const { return snapshot_; }
  BlobGC* blob_gc() const { return blob_gc_.get(); }

 private:
  uint32_t cf_id_;
  std::unique_ptr<BlobGC> blob_gc_;
  DBImpl* base_db_impl_;
  TitanDBOptions db_options_;
  Env* env_;
  EnvOptions env_options_;
  const Snapshot* snapshot_;

  std::atomic_bool* shuting_down_{nullptr};

  std::unordered_map<uint64_t, uint64_t> effective_file_size_map_;
  std::unordered_map<uint64_t, int64_t> disk_usage_map_;

  // TODO: Add more stats

  Status HolePunchBlobFiles();
  Status HolePunchSingleBlobFile(std::shared_ptr<BlobFileMeta> file);
  Status WhetherToPunchHole(const Slice& key, const BlobIndex& blob_index,
                            bool* discardable);
  bool IsShutingDown() {
    return (shuting_down_ && shuting_down_->load(std::memory_order_acquire));
  }
  // REQUIRE: db mutex held
  void UpdateBlobFilesMeta();
  Status Cleanup();
};
}  // namespace titandb
}  // namespace rocksdb