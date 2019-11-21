#pragma once

#include "rocksdb/merge_operator.h"

#include "blob_file_set.h"

#include <iostream>

namespace rocksdb {
namespace titandb {

class BlobIndexMergeOperator : public MergeOperator {
public:
  BlobIndexMergeOperator(port::Mutex* db_mutex, BlobFileSet* blob_file_set,
                         uint32_t cf_id)
      : db_mutex_(db_mutex), blob_file_set_(blob_file_set), cf_id_(cf_id) {}

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    Status s;
    if (merge_in.existing_value) {
      bool use_existing_value = false;
      if (merge_in.value_type == kTypeBlobIndex) {
        Slice copy = *merge_in.existing_value;
        BlobIndex base_index;
        s = base_index.DecodeFrom(&copy);
        if (!s.ok()) {
          return false;
        }
        use_existing_value = !CheckBlobIndexIsObsolete(base_index);
        if (use_existing_value) {
          if (CheckBlobIndexIsObsolete(base_index)) {
            std::cout << "Fatal: apply an obsolete existing ("
                      << base_index.file_number << ")" << std::endl;
          }
        }
      } else if (merge_in.value_type == kTypeValue) {
        use_existing_value = true;
      }
      if (use_existing_value) {
        VersionedBlobIndex index;
        merge_out->new_type = merge_in.value_type;
        merge_out->existing_operand = *merge_in.existing_value;
        for (auto operand : merge_in.operand_list) {
          s = index.DecodeFrom(&operand);
          if (!s.ok()) {
            // non-fatal error
          } else {
            Discard(index);
          }
        }
        return true;
      }
    }

    VersionedBlobIndex index;
    VersionedBlobIndex latest_index;
    SequenceNumber latest_sequence;
    bool filled = false;
    for (auto operand : merge_in.operand_list) {
      s = index.DecodeFrom(&operand);
      if (!s.ok()) {
        return false;
      }
      if (!filled) {
        latest_index = index;
        latest_sequence = index.sequence;
        filled = true;
      } else if (latest_sequence < index.sequence) {
        latest_sequence = index.sequence;
        Discard(latest_index);
        latest_index = index;
      } else if (latest_sequence == index.sequence) {
        // non-fatal error
      } else {
        Discard(index);
      }
    }
    assert(filled);
    if (CheckBlobIndexIsObsolete(latest_index)) {
      // std::cout << "Fatal: apply an obsolete operand (" <<
      // latest_index.file_number << ")" << std::endl;
    }
    merge_out->new_type = kTypeBlobIndex;
    merge_out->new_value.clear();
    latest_index.EncodeToUnversioned(&merge_out->new_value);
    return true;
  }

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* logger) const override {
    // std::cout << "PartialMergeMulti" << std::endl;
    return false;
  }

  const char *Name() const override { return "BlobGCOperator"; }

  void set_column_family_id(uint32_t cf_id) { cf_id_ = cf_id; }

private:
  bool CheckBlobIndexIsObsolete(VersionedBlobIndex& blob_index) const {
    MutexLock l(db_mutex_);
    std::shared_ptr<BlobStorage> blob_storage =
        blob_file_set_->GetBlobStorage(cf_id_).lock();
    if (blob_storage) {
      auto sfile = blob_storage->FindFile(blob_index.file_number).lock();
      return !sfile || sfile->is_obsolete();
    } else {
      // error
      return true;
    }
  }

  void CheckBlobFileExists(BlobIndex& blob_index, bool existing_value) const {
    MutexLock l(db_mutex_);
    std::shared_ptr<BlobStorage> blob_storage =
        blob_file_set_->GetBlobStorage(cf_id_).lock();
    if (blob_storage) {
      auto sfile = blob_storage->FindFile(blob_index.file_number).lock();
      if (!sfile) {
        std::cout << "Check: "
                  << (existing_value ? "existing_value" : "operand")
                  << " Failed from file " << blob_index.file_number
                  << std::endl;
      }
    }
  }

  void CheckBlobFileExists(VersionedBlobIndex& blob_index,
                           bool existing_value) const {
    MutexLock l(db_mutex_);
    std::shared_ptr<BlobStorage> blob_storage =
        blob_file_set_->GetBlobStorage(cf_id_).lock();
    if (blob_storage) {
      auto sfile = blob_storage->FindFile(blob_index.file_number).lock();
      if (!sfile) {
        std::cout << "Check: "
                  << (existing_value ? "existing_value" : "operand")
                  << " Failed from file " << blob_index.file_number
                  << std::endl;
      }
    }
  }

  bool CheckBlobIndexIsObsolete(BlobIndex& blob_index) const {
    MutexLock l(db_mutex_);
    std::shared_ptr<BlobStorage> blob_storage =
        blob_file_set_->GetBlobStorage(cf_id_).lock();
    if (blob_storage) {
      auto sfile = blob_storage->FindFile(blob_index.file_number).lock();
      return !sfile || sfile->is_obsolete();
    } else {
      // error
      return true;
    }
  }

  void Discard(VersionedBlobIndex& blob_index) const {
    // register to global manifest
    // so that onCompactionComplete can update statistics
  }

  port::Mutex* db_mutex_;

  BlobFileSet* blob_file_set_;
  uint32_t cf_id_;
  bool check_unique_ = false;
};

} // namespace titandb
} // namespace rocksdb
