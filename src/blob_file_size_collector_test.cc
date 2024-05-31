#include "blob_file_size_collector.h"

#include "test_util/testharness.h"

namespace rocksdb {
namespace titandb {

class BlobFileSizeCollectorTest : public testing::Test {
 public:
  Env* env_{Env::Default()};
  EnvOptions env_options_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  // Derived options.
  ImmutableDBOptions db_ioptions_;
  MutableCFOptions cf_moptions_;
  ImmutableCFOptions cf_ioptions_;
  ImmutableOptions ioptions_;
  std::shared_ptr<const SliceTransform> prefix_extractor_ = nullptr;

  std::unique_ptr<TableFactory> table_factory_;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>> collectors_;

  std::string tmpdir_;
  std::string file_name_;

  BlobFileSizeCollectorTest()
      : table_factory_(NewBlockBasedTableFactory()),
        tmpdir_(test::TmpDir(env_)),
        file_name_(tmpdir_ + "/TEST") {
    db_options_.dirname = tmpdir_;
    auto blob_file_size_collector_factory =
        std::make_shared<BlobFileSizeCollectorFactory>();
    collectors_.emplace_back(new UserKeyTablePropertiesCollectorFactory(
        blob_file_size_collector_factory));
    // Refresh options.
    db_ioptions_ = ImmutableDBOptions(db_options_);
    cf_moptions_ = MutableCFOptions(cf_options_);
    cf_ioptions_ = ImmutableCFOptions(cf_options_);
    ioptions_ = ImmutableOptions(db_ioptions_, cf_ioptions_);
  }

  ~BlobFileSizeCollectorTest() {
    env_->DeleteFile(file_name_);
    env_->DeleteDir(tmpdir_);
  }

  void NewFileWriter(std::unique_ptr<WritableFileWriter>* result) {
    std::unique_ptr<FSWritableFile> writable_file;
    ASSERT_OK(env_->GetFileSystem()->NewWritableFile(
        file_name_, FileOptions(env_options_), &writable_file,
        nullptr /*dbg*/));
    result->reset(new WritableFileWriter(std::move(writable_file), file_name_,
                                         FileOptions(env_options_)));
    ASSERT_TRUE(*result);
  }

  void NewTableBuilder(WritableFileWriter* file,
                       std::unique_ptr<TableBuilder>* result) {
    CompressionOptions compression_opts;
    TableBuilderOptions options(
        ioptions_, cf_moptions_, cf_ioptions_.internal_comparator, &collectors_,
        kNoCompression, compression_opts, 0 /*column_family_id*/,
        kDefaultColumnFamilyName, 0 /*level*/);
    result->reset(table_factory_->NewTableBuilder(options, file));
    ASSERT_TRUE(*result);
  }

  void NewFileReader(std::unique_ptr<RandomAccessFileReader>* result) {
    std::unique_ptr<FSRandomAccessFile> file;
    ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(
        file_name_, FileOptions(env_options_), &file, nullptr /*dbg*/));
    result->reset(new RandomAccessFileReader(std::move(file), file_name_,
                                             env_->GetSystemClock().get()));
    ASSERT_TRUE(*result);
  }

  void NewTableReader(std::unique_ptr<RandomAccessFileReader>&& file,
                      std::unique_ptr<TableReader>* result) {
    TableReaderOptions options(ioptions_, prefix_extractor_, env_options_,
                               cf_ioptions_.internal_comparator, 0);
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file->file_name(), &file_size));
    ASSERT_TRUE(file_size > 0);
    ASSERT_OK(table_factory_->NewTableReader(options, std::move(file),
                                             file_size, result));
    ASSERT_TRUE(*result);
  }
};

TEST_F(BlobFileSizeCollectorTest, Basic) {
  std::unique_ptr<WritableFileWriter> wfile;
  NewFileWriter(&wfile);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(wfile.get(), &table_builder);

  constexpr uint64_t kFirstFileNumber = 1ULL;
  constexpr uint64_t kSecondFileNumber = 2ULL;
  const int kNumEntries = 100;
  char buf[16];
  for (int i = 0; i < kNumEntries; i++) {
    ParsedInternalKey ikey;
    snprintf(buf, sizeof(buf), "%15d", i);
    ikey.user_key = buf;
    ikey.type = kTypeBlobIndex;
    std::string key;
    AppendInternalKey(&key, ikey);

    BlobIndex index;
    if (i % 2 == 0) {
      index.file_number = kFirstFileNumber;
    } else {
      index.file_number = kSecondFileNumber;
    }
    index.blob_handle.size = 10;
    std::string value;
    index.EncodeTo(&value);

    table_builder->Add(key, value);
  }
  ASSERT_OK(table_builder->status());
  ASSERT_EQ(kNumEntries, table_builder->NumEntries());
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(wfile->Flush());
  ASSERT_OK(wfile->Sync(true));

  std::unique_ptr<RandomAccessFileReader> rfile;
  NewFileReader(&rfile);
  std::unique_ptr<TableReader> table_reader;
  NewTableReader(std::move(rfile), &table_reader);

  auto table_properties = table_reader->GetTableProperties();
  ASSERT_TRUE(table_properties);
  auto iter = table_properties->user_collected_properties.find(
      BlobFileSizeCollector::kPropertiesName);
  ASSERT_TRUE(iter != table_properties->user_collected_properties.end());

  Slice raw_blob_file_size_prop(iter->second);
  std::map<uint64_t, uint64_t> result;
  BlobFileSizeCollector::Decode(&raw_blob_file_size_prop, &result);

  ASSERT_EQ(2, result.size());

  ASSERT_EQ(kNumEntries / 2 * 10, result[kFirstFileNumber]);
  ASSERT_EQ(kNumEntries / 2 * 10, result[kSecondFileNumber]);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
