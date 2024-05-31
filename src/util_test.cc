#include "util.h"

#include "test_util/testharness.h"

namespace rocksdb {
namespace titandb {

class UtilTest : public testing::Test {};

TEST(UtilTest, Compression) {
  std::string input(1024, 'a');
  for (auto compression :
       {kSnappyCompression, kZlibCompression, kLZ4Compression, kZSTD}) {
    CompressionOptions compression_opt;
    CompressionContext compression_ctx(compression, compression_opt);
    CompressionInfo compression_info(
        compression_opt, compression_ctx, CompressionDict::GetEmptyDict(),
        compression, 0 /* sample_for_compression */);
    std::string buffer;
    auto compressed = Compress(compression_info, input, &buffer, &compression);
    if (compression != kNoCompression) {
      ASSERT_TRUE(compressed.size() <= input.size());
      UncompressionContext uncompression_ctx(compression);
      UncompressionInfo uncompression_info(
          uncompression_ctx, UncompressionDict::GetEmptyDict(), compression);
      OwnedSlice output;
      ASSERT_OK(Uncompress(uncompression_info, compressed, &output));
      ASSERT_EQ(output, input);
    }
  }
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
