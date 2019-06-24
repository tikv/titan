#pragma once

#include <unordered_map>

#include "version_edit.h"
#include "version_set.h"

#include <inttypes.h>

namespace rocksdb {
namespace titandb {

class EditCollector {
 public:
  Status AddEdit(const VersionEdit& edit) {
    auto cf_id = edit.column_family_id_;
    auto& collector = column_families_[cf_id];

    Status s;
    for (auto& file : edit.added_files_) {
      s = collector.AddFile(file);
      if (!s.ok()) return s;
    }
    for (auto& file : edit.deleted_files_) {
      collector.DeleteFile(file.first, file.second);
      if (!s.ok()) return s;
    }

    if (edit.has_next_file_number_) {
      assert(edit.next_file_number_ >= next_file_number_);
      next_file_number_ = edit.next_file_number_;
      has_next_file_number_ = true;
    }
    return Status::OK();
  }

  Status Apply(VersionSet& vset) {
    for (auto& cf : column_families_) {
      auto cf_id = cf.first;
      auto storage = vset.GetBlobStorage(cf_id).lock();
      if (!storage) {
        // TODO: support OpenForReadOnly which doesn't open DB with all column
        // family so there are maybe some invalid column family, but we can't
        // just skip it otherwise blob files of the non-open column families
        // will be regarded as obsolete and deleted.
        continue;
      }
      Status s = cf.second.Apply(storage);
      if (!s.ok()) return s;
    }

    return Status::OK();
  }

  Status GetNextFileNumber(uint64_t* next_file_number) {
    if (has_next_file_number_) {
      *next_file_number = next_file_number_;
      return Status::OK();
    }
    return Status::Corruption("no next file number in manifest file");
  }

 private:
  class CFEditCollector {
   public:
    Status AddFile(const std::shared_ptr<BlobFileMeta>& file) {
      auto number = file->file_number();
      if (added_files_.count(number) > 0) {
        return Status::Corruption("blob file has been added twice");
      }
      added_files_.emplace(number, file);
      return Status::OK();
    }

    Status DeleteFile(uint64_t number, SequenceNumber obsolete_sequence) {
      if (deleted_files_.count(number) > 0) {
        return Status::Corruption("blob file has been deleted twice");
      }
      deleted_files_.emplace(number, obsolete_sequence);
      return Status::OK();
    }

    Status Apply(shared_ptr<BlobStorage>& storage) {
      for (auto& file : deleted_files_) {
        auto number = file.first;
        auto blob = storage->FindFile(number).lock();
        if (!blob) {
          ROCKS_LOG_ERROR(storage->db_options().info_log,
                          "blob file %" PRIu64 " doesn't exist before\n",
                          number);
          return Status::Corruption("blob file doesn't exist before");
        } else if (blob->is_obsolete()) {
          ROCKS_LOG_ERROR(storage->db_options().info_log,
                          "blob file %" PRIu64 " has been deleted already\n",
                          number);
          return Status::Corruption("blob file has been deleted already");
        }
        storage->MarkFileObsolete(blob, file.second);
      }

      for (auto& file : added_files_) {
        auto number = file.first;
        auto blob = storage->FindFile(number).lock();
        if (blob) {
          if (blob->is_obsolete()) {
            ROCKS_LOG_ERROR(storage->db_options().info_log,
                            "blob file %" PRIu64 " has been deleted before\n",
                            number);
            return Status::Corruption("blob file has been deleted before");
          } else {
            ROCKS_LOG_ERROR(storage->db_options().info_log,
                            "blob file %" PRIu64 " has been added before\n",
                            number);
            return Status::Corruption("blob file has been added before");
          }
        }
        storage->AddBlobFile(file.second);
      }

      storage->ComputeGCScore();
      return Status::OK();
    }

   private:
    std::unordered_map<uint64_t, std::shared_ptr<BlobFileMeta>> added_files_;
    std::unordered_map<uint64_t, SequenceNumber> deleted_files_;
  };

  bool has_next_file_number_{false};
  uint64_t next_file_number_{0};
  std::unordered_map<uint32_t, CFEditCollector> column_families_;
};

}  // namespace titandb
}  // namespace rocksdb