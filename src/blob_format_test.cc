#include "test_util/testharness.h"

#include "blob_format.h"
#include "testutil.h"
#include "util.h"

namespace rocksdb {
namespace titandb {

class BlobFormatTest : public testing::Test {};

TEST(BlobFormatTest, BlobRecord) {
  BlobRecord input;
  CheckCodec(input);
  input.key = "hello";
  input.value = "world";
  CheckCodec(input);
}

TEST(BlobFormatTest, BlobHandle) {
  BlobHandle input;
  CheckCodec(input);
  input.offset = 2;
  input.size = 3;
  CheckCodec(input);
}

TEST(BlobFormatTest, BlobIndex) {
  BlobIndex input;
  CheckCodec(input);
  input.file_number = 1;
  input.blob_handle.offset = 2;
  input.blob_handle.size = 3;
  CheckCodec(input);
}

TEST(BlobFormatTest, BlobFileMeta) {
  BlobFileMeta input(2, 3, 0, 0, "0", "9");
  CheckCodec(input);
}

TEST(BlobFormatTest, BlobFileFooter) {
  BlobFileFooter input;
  CheckCodec(input);
  input.meta_index_handle.set_offset(123);
  input.meta_index_handle.set_size(321);
  CheckCodec(input);
}

TEST(BlobFormatTest, BlobFileStateTransit) {
  BlobFileMeta blob_file;
  ASSERT_EQ(blob_file.file_state(), BlobFileMeta::FileState::kInit);
  blob_file.FileStateTransit(BlobFileMeta::FileEvent::kDbRestart);
  ASSERT_EQ(blob_file.file_state(), BlobFileMeta::FileState::kNormal);
  blob_file.FileStateTransit(BlobFileMeta::FileEvent::kGCBegin);
  ASSERT_EQ(blob_file.file_state(), BlobFileMeta::FileState::kBeingGC);
  blob_file.FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);

  BlobFileMeta compaction_output;
  ASSERT_EQ(compaction_output.file_state(), BlobFileMeta::FileState::kInit);
  compaction_output.FileStateTransit(
      BlobFileMeta::FileEvent::kFlushOrCompactionOutput);
  ASSERT_EQ(compaction_output.file_state(),
            BlobFileMeta::FileState::kPendingLSM);
  compaction_output.FileStateTransit(
      BlobFileMeta::FileEvent::kCompactionCompleted);
  ASSERT_EQ(compaction_output.file_state(), BlobFileMeta::FileState::kNormal);
}

TEST(BlobFormatTest, BlobCompressionLZ4) {
  BlobEncoder encoder(kLZ4Compression);
  BlobDecoder decoder;

  BlobRecord record;
  record.key = "key1";
  record.value = "value1";

  encoder.EncodeRecord(record);
  Slice encoded_record = encoder.GetRecord();
  Slice encoded_header = encoder.GetHeader();

  decoder.DecodeHeader(&encoded_header);

  BlobRecord decoded_record;
  OwnedSlice blob;
  decoder.DecodeRecord(&encoded_record, &decoded_record, &blob);

  ASSERT_EQ(record, decoded_record);
}

#if defined(ZSTD)

std::string CreateDict() {
  const int sample_count = 1000;
  std::string samples = "";
  std::vector<size_t> sample_lens;

  BlobRecord record;
  BlobEncoder encoder(kZSTD);

  for (int i = 0; i < sample_count; ++i) {
    record.key = "key" + std::to_string(i);
    record.value = "value" + std::to_string(i);
    encoder.EncodeRecord(record);

    std::string encoded_record = encoder.GetRecord().ToString();
    sample_lens.push_back(encoded_record.size());
    samples += encoded_record;
  }

  return ZSTD_TrainDictionary(samples, sample_lens, 4000);
}

TEST(BlobFormatTest, BlobCompressionZSTD) {
  auto dict = CreateDict();
  CompressionDict compression_dict(dict, kZSTD, 10);
  UncompressionDict uncompression_dict(dict, true);

  BlobEncoder encoder(kZSTD, compression_dict);
  BlobDecoder decoder(uncompression_dict);

  BlobRecord record;
  record.key = "key1";
  record.value = "value1";

  encoder.EncodeRecord(record);
  Slice encoded_record = encoder.GetRecord();
  Slice encoded_header = encoder.GetHeader();

  decoder.DecodeHeader(&encoded_header);

  BlobRecord decoded_record;
  OwnedSlice blob;
  decoder.DecodeRecord(&encoded_record, &decoded_record, &blob);

  ASSERT_EQ(record, decoded_record);
}

#endif  // ZSTD

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
