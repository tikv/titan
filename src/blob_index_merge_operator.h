#pragma once

#include "rocksdb/merge_operator.h"

#include "blob_file_set.h"

namespace rocksdb {
namespace titandb {

class BlobIndexMergeOperator : public MergeOperator {
 public:
  BlobIndexMergeOperator() = default;

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    Status s;
    MergeBlobIndex index;
    if (merge_in.existing_value && merge_in.value_type == kTypeValue) {
      merge_out->new_type = merge_in.value_type;
      merge_out->existing_operand = *merge_in.existing_value;
      for (auto operand : merge_in.operand_list) {
        s = index.DecodeFrom(&operand);
        if (!s.ok()) {
          return false;
        }
      }
      return true;
    }

    BlobIndex existing_index;
    bool existing_index_valid = false;
    if (merge_in.existing_value) {
      assert(merge_in.value_type == kTypeBlobIndex);
      Slice copy = *merge_in.existing_value;
      s = existing_index.DecodeFrom(&copy);
      if (!s.ok()) {
        return false;
      }
      existing_index_valid = true;
    }

    BlobIndex merge_index;
    SequenceNumber latest_sequence;
    bool filled = false;
    for (auto operand : merge_in.operand_list) {
      s = index.DecodeFrom(&operand);
      if (!s.ok()) {
        return false;
      }
      // if any merge is sourced from base index, then the base index must
      // be stale.
      if (existing_index_valid &&
          index.source_file_number == existing_index.file_number) {
        // Discard(existing_index);
        existing_index_valid = false;
      }
      // if base index is still valid, merges must be sourced from older
      // modifies.
      if (!existing_index_valid) {
        if (!filled) {
          merge_index = index;
          latest_sequence = index.sequence;
          filled = true;
        } else if (latest_sequence <= index.sequence) {
          // notice the equal case here, merge on merge is possible
          latest_sequence = index.sequence;
          // Discard(merge_index);
          merge_index = index;
        } else {
          // the merge comes from an older modify.
        }
      }
    }
    merge_out->new_type = kTypeBlobIndex;
    merge_out->new_value.clear();
    if (existing_index_valid) {
      existing_index.EncodeTo(&merge_out->new_value);
    } else {
      assert(filled);
      merge_index.EncodeTo(&merge_out->new_value);
    }
    return true;
  }

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* logger) const override {
    return false;
  }

  const char* Name() const override { return "BlobGCOperator"; }
};

}  // namespace titandb
}  // namespace rocksdb
