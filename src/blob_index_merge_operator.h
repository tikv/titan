#pragma once

#include "rocksdb/merge_operator.h"

#include "blob_file_set.h"

#include <iostream>

namespace rocksdb {
namespace titandb {

class BlobIndexMergeOperator : public MergeOperator {
public:
  BlobIndexMergeOperator() = default;

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    Status s;
    if (merge_in.existing_value && merge_in.value_type == kTypeValue) {
      MergeBlobIndex index;
      merge_out->new_type = merge_in.value_type;
      merge_out->existing_operand = *merge_in.existing_value;
      for (auto operand : merge_in.operand_list) {
        s = index.DecodeFrom(&operand);
        if (!s.ok()) {
          // non-fatal error
        }
      }
      return true;
    }

    BlobIndex existing_index;
    bool existing_index_valid = false;
    if (merge_in.existing_value) {
      Slice copy = *merge_in.existing_value;
      s = existing_index.DecodeFrom(&copy);
      if (!s.ok()) {
        return false;
      }
      existing_index_valid = true;
    }

    MergeBlobIndex index;
    bool filled = false;
    BlobIndex output_index;
    SequenceNumber latest_sequence;
    for (auto operand : merge_in.operand_list) {
      s = index.DecodeFrom(&operand);
      if (!s.ok()) {
        return false;
      }
      if (existing_index_valid &&
          index.source_file_number == existing_index.file_number) {
        // Discard(existing_index);
        existing_index_valid = false;
      }
      if (!filled) {
        output_index = index;
        latest_sequence = index.sequence;
        filled = true;
      } else if (latest_sequence < index.sequence) {
        latest_sequence = index.sequence;
        // Discard(output_index);
        output_index = index;
      } else if (latest_sequence == index.sequence) {
        // non-fatal error
      } else {
        // Discard(index);
      }
    }
    assert(filled);
    merge_out->new_type = kTypeBlobIndex;
    merge_out->new_value.clear();
    if (existing_index_valid) {
      existing_index.EncodeTo(&merge_out->new_value);
    } else {
      output_index.EncodeTo(&merge_out->new_value);
    }
    return true;
  }

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* logger) const override {
    return false;
  }

  const char *Name() const override { return "BlobGCOperator"; }
private:
  bool check_unique_ = false;
};

} // namespace titandb
} // namespace rocksdb
