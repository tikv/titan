#pragma once

#include <set>

#include "blob_format.h"
#include "rocksdb/slice.h"

#include <inttypes.h>

namespace rocksdb {
namespace titandb {

class VersionEdit {
 public:
  void SetNextFileNumber(uint64_t v) {
    has_next_file_number_ = true;
    next_file_number_ = v;
  }

  void SetColumnFamilyID(uint32_t v) { column_family_id_ = v; }

  void AddBlobFile(std::shared_ptr<BlobFileMeta> file) {
    added_files_.push_back(file);
  }

  void DeleteBlobFile(uint64_t file_number,
                      SequenceNumber obsolete_sequence = 0) {
    deleted_files_.emplace_back(std::make_pair(file_number, obsolete_sequence));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const VersionEdit& lhs, const VersionEdit& rhs);

 private:
  friend class VersionSet;
  friend class EditCollector;

  bool has_next_file_number_{false};
  uint64_t next_file_number_{0};
  uint32_t column_family_id_{0};

  std::vector<std::shared_ptr<BlobFileMeta>> added_files_;
  std::vector<std::pair<uint64_t, SequenceNumber>> deleted_files_;
};

class EditCollector {
 public:
  Status AddEdit(VersionEdit* edit) {
    auto cf_id = edit->column_family_id_;
    auto& collector = column_families_[cf_id];

    Status s;
    for (auto& file : edit->added_files_) {
      s = collector.AddFile(file);
      if (!s.ok()) return s;
    }
    for (auto& file : edit->deleted_files_) {
      collector.DeleteFile(file.first, file.second);
      if (!s.ok()) return s;
    }
    return Status::OK();
  }

 private:
  friend class VersionSet;

  class Collector {
   public:
    Collector() {}

    Status AddFile(const std::shared_ptr<BlobFileMeta>& file) {
      auto number = file->file_number();
      if (added_files_.find(number) != added_files_.end()) {
        return Status::Corruption("blob file has been added before");
      }
      added_files_.emplace(number, file);
      return Status::OK();
    }

    Status DeleteFile(uint64_t number, SequenceNumber obsolete_sequence) {
      if (!added_files_.erase(number)) {
        deleted_files_.emplace_back(std::make_pair(number, obsolete_sequence));
      }
      return Status::OK();
    }

    std::unordered_map<uint64_t, std::shared_ptr<BlobFileMeta>> added_files_;
    std::vector<std::pair<uint64_t, SequenceNumber>> deleted_files_;
  };
  std::unordered_map<uint32_t, Collector> column_families_;
};

}  // namespace titandb
}  // namespace rocksdb
