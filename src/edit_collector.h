#pragma once

#include <unordered_map>

#include "blob_file_set.h"
#include "util/string_util.h"
#include "version_edit.h"

#include <inttypes.h>

namespace rocksdb {
namespace titandb {

// A collector to apply edits in batch.
// The functions should be called in the sequence:
//    AddEdit() -> Seal() -> Apply()
class EditCollector {
 public:
  // Add the edit into the batch.
  Status AddEdit(const VersionEdit& edit) {
    if (sealed_)
      return Status::Incomplete(
          "Should be not called after Sealed() is called");

    auto cf_id = edit.column_family_id_;
    auto& collector = column_families_[cf_id];

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
        status_ =
            Status::Corruption("Edit has a smaller next file number " +
                               ToString(edit.next_file_number_) +
                               " than current " + ToString(next_file_number_));
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

 private:
  class CFEditCollector {
   public:
    Status AddFile(const std::shared_ptr<BlobFileMeta>& file) {
      auto number = file->file_number();
      if (added_files_.count(number) > 0) {
        return Status::Corruption("Blob file " + ToString(number) +
                                  " has been added twice");
      }
      added_files_.emplace(number, file);
      return Status::OK();
    }

    Status DeleteFile(uint64_t number, SequenceNumber obsolete_sequence) {
      if (deleted_files_.count(number) > 0) {
        return Status::Corruption("Blob file " + ToString(number) +
                                  " has been deleted twice");
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
            ROCKS_LOG_ERROR(storage->db_options().info_log,
                            "blob file %" PRIu64 " has been deleted before\n",
                            number);
            return Status::Corruption("Blob file " + ToString(number) +
                                      " has been deleted before");
          } else {
            ROCKS_LOG_ERROR(storage->db_options().info_log,
                            "blob file %" PRIu64 " has been added before\n",
                            number);
            return Status::Corruption("Blob file " + ToString(number) +
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
          ROCKS_LOG_ERROR(storage->db_options().info_log,
                          "blob file %" PRIu64 " doesn't exist before\n",
                          number);
          return Status::Corruption("Blob file " + ToString(number) +
                                    " doesn't exist before");
        } else if (blob->is_obsolete()) {
          ROCKS_LOG_ERROR(storage->db_options().info_log,
                          "blob file %" PRIu64 " has been deleted already\n",
                          number);
          return Status::Corruption("Blob file " + ToString(number) +
                                    " has been deleted already");
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

   private:
    std::unordered_map<uint64_t, std::shared_ptr<BlobFileMeta>> added_files_;
    std::unordered_map<uint64_t, SequenceNumber> deleted_files_;
  };

  Status status_{Status::OK()};
  //
  bool sealed_{false};
  bool has_next_file_number_{false};
  uint64_t next_file_number_{0};
  std::unordered_map<uint32_t, CFEditCollector> column_families_;
};

}  // namespace titandb
}  // namespace rocksdb