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

  void UpdateInternalOpStats();

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
    uint64_t blob_db_bytes_read = 0;
    uint64_t blob_db_bytes_written = 0;
    uint64_t blob_db_gc_num_keys_overwritten = 0;
    uint64_t blob_db_gc_bytes_overwritten = 0;
    uint64_t blob_db_gc_num_keys_relocated = 0;
    uint64_t blob_db_gc_bytes_relocated = 0;
    uint64_t blob_db_gc_num_new_files = 0;
    uint64_t blob_db_gc_num_files = 0;
  } metrics_;

  uint64_t prev_bytes_read_ = 0;
  uint64_t prev_bytes_written_ = 0;
  uint64_t io_bytes_read_ = 0;
  uint64_t io_bytes_written_ = 0;

  Status SampleCandidateFiles();
  Status DoSample(const BlobFileMeta* file, bool* selected);
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
