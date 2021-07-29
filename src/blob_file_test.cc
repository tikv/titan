#include <cinttypes>

#include "blob_file_builder.h"
#include "blob_file_cache.h"
#include "blob_file_reader.h"
#include "file/filename.h"
#include "test_util/testharness.h"

namespace rocksdb {
namespace titandb {

class BlobFileTest : public testing::Test {
 public:
  BlobFileTest() : dirname_(test::TmpDir(env_)) {
    file_name_ = BlobFileName(dirname_, file_number_);
  }

  ~BlobFileTest() {
    env_->DeleteFile(file_name_);
    env_->DeleteDir(dirname_);
  }

  std::string GenKey(uint64_t i) {
    char buf[64];
    snprintf(buf, sizeof(buf), "k-%08" PRIu64, i);
    return buf;
  }

  std::string GenValue(uint64_t i) { return std::string(1024, i); }

  void AddRecord(BlobFileBuilder* builder, BlobRecord& record,
                 BlobFileBuilder::OutContexts& contexts) {
    std::unique_ptr<BlobFileBuilder::BlobRecordContext> ctx(
        new BlobFileBuilder::BlobRecordContext);
    ctx->key = record.key.ToString();
    BlobFileBuilder::OutContexts cur_contexts;
    builder->Add(record, std::move(ctx), &cur_contexts);
    for (size_t i = 0; i < cur_contexts.size(); i++) {
      contexts.emplace_back(std::move(cur_contexts[i]));
    }
  }

  Status Finish(BlobFileBuilder* builder,
                BlobFileBuilder::OutContexts& contexts) {
    BlobFileBuilder::OutContexts cur_contexts;
    Status s = builder->Finish(&cur_contexts);
    for (size_t i = 0; i < cur_contexts.size(); i++) {
      contexts.emplace_back(std::move(cur_contexts[i]));
    }
    return s;
  }

  void TestBlobFilePrefetcher(TitanOptions options) {
    options.dirname = dirname_;
    TitanDBOptions db_options(options);
    TitanCFOptions cf_options(options);
    BlobFileCache cache(db_options, cf_options, {NewLRUCache(128)}, nullptr);

    const int n = 100;
    BlobFileBuilder::OutContexts contexts;

    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      ASSERT_OK(env_->NewWritableFile(file_name_, &f, env_options_));
      file.reset(
          new WritableFileWriter(std::move(f), file_name_, env_options_));
    }
    std::unique_ptr<BlobFileBuilder> builder(
        new BlobFileBuilder(db_options, cf_options, file.get()));

    for (int i = 0; i < n; i++) {
      auto key = GenKey(i);
      auto value = GenValue(i);
      BlobRecord record;
      record.key = key;
      record.value = value;

      AddRecord(builder.get(), record, contexts);

      ASSERT_OK(builder->status());
    }
    ASSERT_OK(Finish(builder.get(), contexts));
    ASSERT_OK(builder->status());

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file_name_, &file_size));

    ReadOptions ro;
    std::unique_ptr<BlobFilePrefetcher> prefetcher;
    ASSERT_OK(cache.NewPrefetcher(file_number_, file_size, &prefetcher));
    ASSERT_EQ(contexts.size(), n);
    for (int i = 0; i < n; i++) {
      auto key = GenKey(i);
      auto value = GenValue(i);
      BlobRecord expect;
      expect.key = key;
      expect.value = value;
      BlobRecord record;
      PinnableSlice buffer;
      BlobHandle blob_handle = contexts[i]->new_blob_index.blob_handle;
      ASSERT_OK(cache.Get(ro, file_number_, file_size, blob_handle, &record,
                          &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(cache.Get(ro, file_number_, file_size, blob_handle, &record,
                          &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(prefetcher->Get(ro, blob_handle, &record, &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(prefetcher->Get(ro, blob_handle, &record, &buffer));
      ASSERT_EQ(record, expect);
    }
  }

  void TestBlobFileReader(TitanOptions options) {
    options.dirname = dirname_;
    TitanDBOptions db_options(options);
    TitanCFOptions cf_options(options);
    BlobFileCache cache(db_options, cf_options, {NewLRUCache(128)}, nullptr);

    const int n = 100;
    BlobFileBuilder::OutContexts contexts;

    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      ASSERT_OK(env_->NewWritableFile(file_name_, &f, env_options_));
      file.reset(
          new WritableFileWriter(std::move(f), file_name_, env_options_));
    }
    std::unique_ptr<BlobFileBuilder> builder(
        new BlobFileBuilder(db_options, cf_options, file.get()));

    for (int i = 0; i < n; i++) {
      auto key = GenKey(i);
      auto value = GenValue(i);
      BlobRecord record;
      record.key = key;
      record.value = value;

      AddRecord(builder.get(), record, contexts);

      ASSERT_OK(builder->status());
    }

    ASSERT_OK(Finish(builder.get(), contexts));
    ASSERT_OK(builder->status());

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file_name_, &file_size));

    ReadOptions ro;
    std::unique_ptr<RandomAccessFileReader> random_access_file_reader;
    ASSERT_OK(NewBlobFileReader(file_number_, 0, db_options, env_options_, env_,
                                &random_access_file_reader));
    std::unique_ptr<BlobFileReader> blob_file_reader;
    ASSERT_OK(BlobFileReader::Open(cf_options,
                                   std::move(random_access_file_reader),
                                   file_size, &blob_file_reader, nullptr));
    ASSERT_EQ(contexts.size(), n);

    for (int i = 0; i < n; i++) {
      auto key = GenKey(i);
      auto value = GenValue(i);
      BlobRecord expect;
      expect.key = key;
      expect.value = value;
      BlobRecord record;
      PinnableSlice buffer;
      BlobHandle blob_handle = contexts[i]->new_blob_index.blob_handle;
      ASSERT_OK(cache.Get(ro, file_number_, file_size, blob_handle, &record,
                          &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(cache.Get(ro, file_number_, file_size, blob_handle, &record,
                          &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(blob_file_reader->Get(ro, blob_handle, &record, &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(blob_file_reader->Get(ro, blob_handle, &record, &buffer));
      ASSERT_EQ(record, expect);
    }
  }

  Env* env_{Env::Default()};
  EnvOptions env_options_;
  std::string dirname_;
  std::string file_name_;
  uint64_t file_number_{1};
};

TEST_F(BlobFileTest, BlobFileReader) {
  TitanOptions options;
  TestBlobFileReader(options);
  options.blob_file_compression = kLZ4Compression;
  TestBlobFileReader(options);
}

TEST_F(BlobFileTest, BlobFilePrefetcher) {
  TitanOptions options;
  TestBlobFilePrefetcher(options);
  options.blob_cache = NewLRUCache(1 << 20);
  TestBlobFilePrefetcher(options);
  options.blob_file_compression = kLZ4Compression;
  TestBlobFilePrefetcher(options);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
