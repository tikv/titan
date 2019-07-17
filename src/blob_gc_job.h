#pragma once

#include "blob_file_builder.h"
#include "blob_file_iterator.h"
#include "blob_file_manager.h"
#include "blob_gc.h"
#include "db/db_impl.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "titan/options.h"
#include "titan_stats.h"
#include "version_edit.h"
#include "version_set.h"

namespace rocksdb {
namespace titandb {

class BlobGCJob {
 public:
  BlobGCJob(BlobGC* blob_gc, DB* db, port::Mutex* mutex,
            const TitanDBOptions& titan_db_options, Env* env,
            const EnvOptions& env_options, BlobFileManager* blob_file_manager,
            VersionSet* version_set, LogBuffer* log_buffer,
            std::atomic_bool* shuting_down, TitanStats* stats);

  // No copying allowed
  BlobGCJob(const BlobGCJob&) = delete;
  void operator=(const BlobGCJob&) = delete;

  ~BlobGCJob();

  // REQUIRE: mutex held
  Status Prepare();
  // REQUIRE: mutex not held
  Status Run();
  // REQUIRE: mutex held
  Status Finish();

 private:
  class GarbageCollectionWriteCallback;
  friend class BlobGCJobTest;

  BlobGC* blob_gc_;
  DB* base_db_;
  DBImpl* base_db_impl_;
  port::Mutex* mutex_;
  TitanDBOptions db_options_;
  Env* env_;
  EnvOptions env_options_;
  BlobFileManager* blob_file_manager_;
  VersionSet* version_set_;
  LogBuffer* log_buffer_{nullptr};

  std::vector<std::pair<std::unique_ptr<BlobFileHandle>,
                        std::unique_ptr<BlobFileBuilder>>>
      blob_file_builders_;
  std::vector<std::pair<WriteBatch, GarbageCollectionWriteCallback>>
      rewrite_batches_;

  std::atomic_bool* shuting_down_{nullptr};

  TitanStats* stats_;

  struct {
    uint64_t blob_db_bytes_read;
    uint64_t blob_db_bytes_written;
    uint64_t blob_db_gc_num_keys_overwritten;
    uint64_t blob_db_gc_bytes_overwritten;
    uint64_t blob_db_gc_num_keys_relocated;
    uint64_t blob_db_gc_bytes_relocated;
    uint64_t blob_db_gc_num_new_files;
    uint64_t blob_db_gc_num_files;
  } metrics_;

  Status SampleCandidateFiles();
  Status DoSample(const BlobFileMeta* file, bool* selected,
                  uint64_t& estimate_disacrdable_size);
  Status DoRunGC();
  Status BuildIterator(std::unique_ptr<BlobFileMergeIterator>* result);
  Status DiscardEntry(const Slice& key, const BlobIndex& blob_index,
                      bool* discardable);
  Status InstallOutputBlobFiles();
  Status RewriteValidKeyToLSM();
  Status DeleteInputBlobFiles();

  bool IsShutingDown();
};

}  // namespace titandb
}  // namespace rocksdb
