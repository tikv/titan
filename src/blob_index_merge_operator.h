#pragma once

#include "rocksdb/merge_operator.h"

#include "blob_file_set.h"

namespace rocksdb {
namespace titandb {

class BlobIndexMergeOperator : public MergeOperator {
 public:
  BlobIndexMergeOperator() = default;

  // FullMergeV2 merges one base value with multiple merge operands and
  // preserves latest value w.r.t. timestamp of original *put*. Each merge
  // is the output of blob GC, and contains meta data including *src-file-no*
  // and *src-file-offset*.
  // Merge operation follows such rules:
  // *. basic rule (keep base value): [Y][Z] ... [X](Y)(Z) => [X]
  // a. same put (keep merge value): [Y] ... [X](Y)(X')(X") => [X"]
  //    we identify this case by checking *src-location* of merges against
  //    *blob-handle* of base.
  // b. deletion (keep deletion marker): [delete](X)(Y) => [deletion marker]
  //    this is a workaround since vanilla rocksdb disallow empty result from
  //    merge.
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    Status s;
    if (merge_in.existing_value && merge_in.value_type == kValue) {
      merge_out->new_type = kValue;
      merge_out->existing_operand = *merge_in.existing_value;
      return true;
    }

    BlobIndex existing_index;
    bool existing_index_valid = false;
    if (merge_in.existing_value) {
      assert(merge_in.value_type == kBlobIndex);
      Slice copy = *merge_in.existing_value;
      s = existing_index.DecodeFrom(&copy);
      if (!s.ok()) {
        return false;
      }
      existing_index_valid = !BlobIndex::IsDeletionMarker(existing_index);
    }
    if (!existing_index_valid) {
      // this key must be deleted
      merge_out->new_type = kBlobIndex;
      merge_out->new_value.clear();
      BlobIndex::EncodeDeletionMarkerTo(&merge_out->new_value);
      return true;
    }

    MergeBlobIndex index;
    BlobIndex merge_index;
    for (auto operand : merge_in.operand_list) {
      s = index.DecodeFrom(&operand);
      if (!s.ok()) {
        return false;
      }
      if (existing_index_valid) {
        if (index.source_file_number == existing_index.file_number &&
            index.source_file_offset == existing_index.blob_handle.offset) {
          existing_index_valid = false;
          merge_index = index;
        }
      } else if (index.source_file_number == merge_index.file_number &&
                 index.source_file_offset == merge_index.blob_handle.offset) {
        merge_index = index;
      }
    }
    merge_out->new_type = kBlobIndex;
    if (existing_index_valid) {
      merge_out->existing_operand = *merge_in.existing_value;
    } else {
      merge_out->new_value.clear();
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
