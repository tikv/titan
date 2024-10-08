#pragma once

#include <cinttypes>

#include <algorithm>
#include <unordered_map>

#include "util/string_util.h"

#include "blob_file_set.h"
#include "titan_logging.h"
#include "version_edit.h"

namespace rocksdb {
namespace titandb {

// A collector to apply edits in batch.
// The functions should be called in the sequence:
//    AddEdit() -> Seal() -> Apply()
class EditCollector {
 public:
  EditCollector(Logger* logger, bool _paranoid_check)
      : paranoid_check_(_paranoid_check), info_log_(logger) {}

  // Add the edit into the batch.
  Status AddEdit(const VersionEdit& edit) {
    if (sealed_)
      return Status::Incomplete(
          "Should be not called after Sealed() is called");

    auto cf_id = edit.column_family_id_;
    if (column_families_.find(cf_id) == column_families_.end()) {
      column_families_.emplace(cf_id,
                               CFEditCollector(info_log_, paranoid_check_));
    }
    auto& collector = column_families_.at(cf_id);

    for (auto& file : edit.added_files_) {
      status_ = collector.AddFile(file);
      if (!status_.ok()) return status_;
    }
    for (auto& file : edit.deleted_files_) {
      status_ = collector.DeleteFile(file.first, file.second);
      if (!status_.ok()) return status_;
    }

    if (edit.has_next_file_number_) {
      if (edit.next_file_number_ < next_file_number_) {
        status_ = Status::Corruption("Edit has a smaller next file number " +
                                     std::to_string(edit.next_file_number_) +
                                     " than current " +
                                     std::to_string(next_file_number_));
        return status_;
      }
      next_file_number_ = edit.next_file_number_;
      has_next_file_number_ = true;
    }
    return Status::OK();
  }

  // Seal the batch and check the validation of the edits.
  Status Seal(BlobFileSet& blob_file_set) {
    if (!status_.ok()) return status_;

    for (auto& cf : column_families_) {
      auto cf_id = cf.first;
      auto storage = blob_file_set.GetBlobStorage(cf_id).lock();
      if (!storage) {
        // TODO: support OpenForReadOnly which doesn't open DB with all column
        // family so there are maybe some invalid column family, but we can't
        // just skip it otherwise blob files of the non-open column families
        // will be regarded as obsolete and deleted.
        continue;
      }
      status_ = cf.second.Seal(storage.get());
      if (!status_.ok()) return status_;
    }

    sealed_ = true;
    return Status::OK();
  }

  // Apply the edits of the batch.
  Status Apply(BlobFileSet& blob_file_set) {
    if (!status_.ok()) return status_;
    if (!sealed_)
      return Status::Incomplete(
          "Should be not called until Sealed() is called");

    for (auto& cf : column_families_) {
      auto cf_id = cf.first;
      auto storage = blob_file_set.GetBlobStorage(cf_id).lock();
      if (!storage) {
        // TODO: support OpenForReadOnly which doesn't open DB with all column
        // family so there are maybe some invalid column family, but we can't
        // just skip it otherwise blob files of the non-open column families
        // will be regarded as obsolete and deleted.
        continue;
      }
      status_ = cf.second.Apply(storage.get());
      if (!status_.ok()) return status_;
    }

    return Status::OK();
  }

  Status GetNextFileNumber(uint64_t* next_file_number) {
    if (!status_.ok()) return status_;

    if (has_next_file_number_) {
      *next_file_number = next_file_number_;
      return Status::OK();
    }
    return Status::Corruption("No next file number in manifest file");
  }

  void Dump(bool with_keys) const {
    if (has_next_file_number_) {
      fprintf(stdout, "next_file_number: %" PRIu64 "\n", next_file_number_);
      for (auto& cf : column_families_) {
        fprintf(stdout, "column family: %" PRIu32 "\n", cf.first);
        cf.second.Dump(with_keys);
        fprintf(stdout, "\n");
      }
    }
  }

 private:
  class CFEditCollector {
   public:
    CFEditCollector(Logger* logger, bool _paranoid_check)
        : paranoid_check_(_paranoid_check), info_log_(logger) {}

    Status AddFile(const std::shared_ptr<BlobFileMeta>& file) {
      auto number = file->file_number();
      if (added_files_.count(number) > 0) {
        TITAN_LOG_ERROR(info_log_,
                        "blob file %" PRIu64 " has been deleted twice\n",
                        number);
        if (paranoid_check_) {
          return Status::Corruption("Blob file " + std::to_string(number) +
                                    " has been added twice");
        } else {
          return Status::OK();
        }
      }
      added_files_.emplace(number, file);
      return Status::OK();
    }

    Status DeleteFile(uint64_t number, SequenceNumber obsolete_sequence) {
      if (deleted_files_.count(number) > 0) {
        TITAN_LOG_ERROR(info_log_,
                        "blob file %" PRIu64 " has been deleted twice\n",
                        number);
        if (paranoid_check_) {
          return Status::Corruption("Blob file " + std::to_string(number) +
                                    " has been deleted twice");
        } else {
          return Status::OK();
        }
      }
      deleted_files_.emplace(number, obsolete_sequence);
      return Status::OK();
    }

    Status Seal(BlobStorage* storage) {
      for (auto& file : added_files_) {
        auto number = file.first;
        auto blob = storage->FindFile(number).lock();
        if (blob) {
          if (blob->is_obsolete()) {
            TITAN_LOG_ERROR(storage->db_options().info_log,
                            "blob file %" PRIu64 " has been deleted before\n",
                            number);
            return Status::Corruption("Blob file " + std::to_string(number) +
                                      " has been deleted before");
          } else {
            TITAN_LOG_ERROR(storage->db_options().info_log,
                            "blob file %" PRIu64 " has been added before\n",
                            number);

            return Status::Corruption("Blob file " + std::to_string(number) +
                                      " has been added before");
          }
        }
      }

      for (auto& file : deleted_files_) {
        auto number = file.first;
        if (added_files_.count(number) > 0) {
          continue;
        }
        auto blob = storage->FindFile(number).lock();
        if (!blob) {
          TITAN_LOG_ERROR(storage->db_options().info_log,
                          "blob file %" PRIu64 " doesn't exist before\n",
                          number);
          return Status::Corruption("Blob file " + std::to_string(number) +
                                    " doesn't exist before");
        } else if (blob->is_obsolete()) {
          TITAN_LOG_ERROR(storage->db_options().info_log,
                          "blob file %" PRIu64 " has been deleted already\n",
                          number);
          if (paranoid_check_) {
            return Status::Corruption("Blob file " + std::to_string(number) +
                                      " has been deleted already");
          }
        }
      }

      return Status::OK();
    }

    Status Apply(BlobStorage* storage) {
      for (auto& file : added_files_) {
        // just skip paired added and deleted files
        if (deleted_files_.count(file.first) > 0) {
          continue;
        }
        storage->AddBlobFile(file.second);
      }

      for (auto& file : deleted_files_) {
        auto number = file.first;
        // just skip paired added and deleted files
        if (added_files_.count(number) > 0) {
          continue;
        }
        if (!storage->MarkFileObsolete(number, file.second)) {
          return Status::NotFound("Invalid file number " +
                                  std::to_string(number));
        }
      }

      storage->ComputeGCScore();
      return Status::OK();
    }

    void Dump(bool with_keys) const {
      std::vector<uint64_t> files;
      files.reserve(added_files_.size());
      for (auto& file : added_files_) {
        files.push_back(file.first);
      }
      std::sort(files.begin(), files.end());
      for (uint64_t file : files) {
        if (deleted_files_.count(file) == 0) {
          added_files_.at(file)->Dump(with_keys);
        }
      }
      bool has_additional_deletion = false;
      for (auto& file : deleted_files_) {
        if (added_files_.count(file.first) == 0) {
          if (!has_additional_deletion) {
            fprintf(stdout, "additional deletion:\n");
            has_additional_deletion = true;
          }
          fprintf(stdout, "file %" PRIu64 ", seq %" PRIu64 "\n", file.first,
                  file.second);
        }
      }
    }

   private:
    bool paranoid_check_{false};
    Logger* info_log_{nullptr};
    std::unordered_map<uint64_t, std::shared_ptr<BlobFileMeta>> added_files_;
    std::unordered_map<uint64_t, SequenceNumber> deleted_files_;
  };

  Status status_{Status::OK()};

  // Paranoid check would not pass if a blob file is deleted or added twice.
  bool paranoid_check_{false};
  Logger* info_log_{nullptr};

  bool sealed_{false};
  bool has_next_file_number_{false};
  uint64_t next_file_number_{0};
  std::unordered_map<uint32_t, CFEditCollector> column_families_;
};

}  // namespace titandb
}  // namespace rocksdb
