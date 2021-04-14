#pragma once

#include "titan/checkpoint.h"

#include "file/filename.h"

namespace rocksdb {
namespace titandb {

class TitanCheckpointImpl : public Checkpoint {
 public:
  // Creates a Checkpoint object to be used for creating openable snapshots
  explicit TitanCheckpointImpl(TitanDB* db) : db_(db) {}

  // Builds an openable snapshot of TitanDB on the same disk, which
  // accepts an output directory on the same disk, and under the directory
  // (1) hard-linked SST files pointing to existing live SST files
  // SST files will be copied if output directory is on a different filesystem
  // (2) a copied manifest files and other files
  // The directory should not already exist and will be created by this API.
  // The directory will be an absolute path
  using Checkpoint::CreateCheckpoint;
  virtual Status CreateCheckpoint(const std::string& checkpoint_dir,
                                  uint64_t log_size_for_flush) override;


  // Checkpoint logic can be customized by providing callbacks for link, copy,
  // or create.
  Status CreateCustomCheckpoint(
          const DBOptions& db_options,
          std::function<Status(const std::string& src_dirname,
                               const std::string& fname, FileType type)>
          link_file_cb,
          std::function<Status(const std::string& src_dirname,
                               const std::string& fname, uint64_t size_limit_bytes,
                               FileType type)>
          copy_file_cb,
          std::function<Status(const std::string& fname,
                               const std::string& contents, FileType type)>
          create_file_cb,
          uint64_t* sequence_number, uint64_t log_size_for_flush);

 private:
  void CleanStagingDirectory(const std::string& path, Logger* info_log);

 private:
  TitanDB* db_;
};

}  // namespace titandb
}  // namespace rocksdb
