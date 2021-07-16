#include "blob_index_merge_operator.h"

#include "test_util/testharness.h"

namespace rocksdb {
namespace titandb {

std::string GenKey(int i) {
  char buffer[32];
  snprintf(buffer, sizeof(buffer), "k-%08d", i);
  return buffer;
}

std::string GenValue(int i) {
  char buffer[32];
  snprintf(buffer, sizeof(buffer), "v-%08d", i);
  return buffer;
}

BlobIndex GenBlobIndex(uint32_t i, uint32_t j = 0) {
  BlobIndex index;
  index.file_number = i;
  index.blob_handle.offset = j;
  index.blob_handle.size = 10;
  return index;
}

MergeBlobIndex GenMergeBlobIndex(BlobIndex src, uint32_t i, uint32_t j = 0) {
  MergeBlobIndex index;
  index.file_number = i;
  index.blob_handle.offset = j;
  index.blob_handle.size = 10;
  index.source_file_number = src.file_number;
  index.source_file_offset = src.blob_handle.offset;
  return index;
}

ValueType ToValueType(MergeOperator::MergeValueType value_type) {
  switch (value_type) {
    case MergeOperator::kDeletion:
      return kTypeDeletion;
    case MergeOperator::kValue:
      return kTypeValue;
    case MergeOperator::kBlobIndex:
      return kTypeBlobIndex;
    default:
      return kTypeValue;
  }
}

MergeOperator::MergeValueType ToMergeValueType(ValueType value_type) {
  switch (value_type) {
    case kTypeDeletion:
    case kTypeSingleDeletion:
    case kTypeRangeDeletion:
      return MergeOperator::kDeletion;
    case kTypeValue:
      return MergeOperator::kValue;
    case kTypeBlobIndex:
      return MergeOperator::kBlobIndex;
    default:
      return MergeOperator::kValue;
  }
}

class BlobIndexMergeOperatorTest : public testing::Test {
 public:
  std::string key_;
  ValueType value_type_{kTypeDeletion};
  std::string value_;
  std::vector<std::string> operands_;
  std::shared_ptr<BlobIndexMergeOperator> merge_operator_;

  BlobIndexMergeOperatorTest()
      : key_("k"),
        merge_operator_(std::make_shared<BlobIndexMergeOperator>()) {}

  void Put(std::string value, ValueType type = kTypeValue) {
    value_ = value;
    value_type_ = type;
    operands_.clear();
  }

  void Put(BlobIndex blob_index) {
    value_.clear();
    blob_index.EncodeTo(&value_);
    value_type_ = kTypeBlobIndex;
    operands_.clear();
  }

  void Merge(MergeBlobIndex blob_index) {
    std::string tmp;
    blob_index.EncodeTo(&tmp);
    operands_.emplace_back(tmp);
  }

  void Read(ValueType expect_type, std::string expect_value) {
    std::string tmp_result_string;
    Slice tmp_result_operand(nullptr, 0);
    MergeOperator::MergeValueType merge_type = ToMergeValueType(value_type_);
    Slice value = value_;
    std::vector<Slice> operands;
    for (auto& op : operands_) {
      operands.emplace_back(op);
    }
    const MergeOperator::MergeOperationInput merge_in(
        key_, merge_type,
        merge_type == MergeOperator::kDeletion ? nullptr : &value, operands,
        nullptr);
    MergeOperator::MergeOperationOutput merge_out(tmp_result_string,
                                                  tmp_result_operand);

    ASSERT_EQ(true, merge_operator_->FullMergeV2(merge_in, &merge_out));
    ASSERT_EQ(true, merge_out.new_type != MergeOperator::kDeletion);

    if (merge_out.new_type == merge_type) {
      ASSERT_EQ(expect_type, value_type_);
    } else {
      ASSERT_EQ(expect_type, ToValueType(merge_out.new_type));
    }

    if (tmp_result_operand.data()) {
      ASSERT_EQ(expect_value, tmp_result_operand);
    } else {
      ASSERT_EQ(expect_value, tmp_result_string);
    }
  }

  void Clear() {
    value_type_ = kTypeDeletion;
    value_.clear();
    operands_.clear();
  }
};

TEST_F(BlobIndexMergeOperatorTest, KeepBaseValue) {
  // [1] [2] (1->3)
  Put(GenBlobIndex(2));
  Merge(GenMergeBlobIndex(GenBlobIndex(1), 3));
  std::string value;
  GenBlobIndex(2).EncodeTo(&value);
  Read(kTypeBlobIndex, value);
  // [v] (1->2)
  Clear();
  Put(GenValue(1));
  Merge(GenMergeBlobIndex(GenBlobIndex(1), 2));
  Read(kTypeValue, GenValue(1));
}

TEST_F(BlobIndexMergeOperatorTest, KeepLatestMerge) {
  // [1] (1->2) (3->4) (2->5)
  Put(GenBlobIndex(1));
  Merge(GenMergeBlobIndex(GenBlobIndex(1), 2));
  Merge(GenMergeBlobIndex(GenBlobIndex(3), 4));
  Merge(GenMergeBlobIndex(GenBlobIndex(2), 5));
  std::string value;
  GenBlobIndex(5).EncodeTo(&value);
  Read(kTypeBlobIndex, value);
}

TEST_F(BlobIndexMergeOperatorTest, Delete) {
  // [delete] (0->1)
  Merge(GenMergeBlobIndex(GenBlobIndex(0), 1));
  std::string value;
  BlobIndex::EncodeDeletionMarkerTo(&value);
  Read(kTypeBlobIndex, value);
  // [deletion marker] (0->1)
  Clear();
  Put(value, kTypeBlobIndex);
  Merge(GenMergeBlobIndex(GenBlobIndex(0), 1));
  Read(kTypeBlobIndex, value);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
