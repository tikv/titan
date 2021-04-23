#pragma once

#include "file/filename.h"
#include "titan/checkpoint.h"

namespace rocksdb {
namespace titandb {

class VersionEdit;

class TitanCheckpointImpl : public Checkpoint {
 public:
  explicit TitanCheckpointImpl(TitanDB* db) : db_(db) {}

  // Builds an openable snapshot of Titan on the same disk, which accepts
  // an output directory on the same disk, and under the directory.
  // (1) Create base db checkpoint.
  // (2) Hard linked all existing blob files(live + obsolete) if the output
  //     directory is on the same filesystem, and copied otherwise.
  // (3) Create MANIFEST file include all records about existing blob files.
  // (4) Craft CURRENT file manually based on MANIFEST file number.
  // This will include redundant blob files, but hopefully not a lot of them,
  // and on restart Titan will recalculate GC stats and GC out those redundant 
  // blob files.
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
      uint64_t* sequence_number, uint64_t log_size_for_flush,
      const std::string full_private_path);

 private:
  void CleanStagingDirectory(const std::string& path, Logger* info_log);

  // Create titan manifest file based on the content of VersionEdit
  Status CreateTitanManifest(const std::string& file_name,
                             std::vector<VersionEdit>* edits);

 private:
  TitanDB* db_;
};

}  // namespace titandb
}  // namespace rocksdb
