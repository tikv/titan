#include "table/table_builder.h"
#include "file/filename.h"
#include "table/table_reader.h"
#include "test_util/testharness.h"

#include "blob_file_manager.h"
#include "blob_file_reader.h"
#include "table_builder.h"
#include "table_factory.h"
#include "version_set.h"

namespace rocksdb {
namespace titandb {

const uint64_t kMinBlobSize = 128;
const uint64_t kTestFileNumber = 123;
const uint64_t kTargetBlobFileSize = 4096;

class FileManager : public BlobFileManager {
 public:
  FileManager(const TitanDBOptions& db_options, VersionSet* vset)
      : db_options_(db_options), number_(kTestFileNumber), vset_(vset) {}

  Status NewFile(std::unique_ptr<BlobFileHandle>* handle) override {
    auto number = number_.fetch_add(1);
    auto name = BlobFileName(db_options_.dirname, number);
    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      Status s = env_->NewWritableFile(name, &f, env_options_);
      if (!s.ok()) return s;
      file.reset(new WritableFileWriter(std::move(f), name, env_options_));
    }
    handle->reset(new FileHandle(number, name, std::move(file)));
    return Status::OK();
  }

  Status FinishFile(uint32_t /*cf_id*/,
                    std::shared_ptr<BlobFileMeta> file /*file*/,
                    std::unique_ptr<BlobFileHandle>&& handle) override {
    Status s = handle->GetFile()->Sync(true);
    if (s.ok()) {
      s = handle->GetFile()->Close();
    }
    auto storage = vset_->GetBlobStorage(0).lock();
    storage->AddBlobFile(file);
    return s;
  }

  Status DeleteFile(std::unique_ptr<BlobFileHandle>&& handle) override {
    return env_->DeleteFile(handle->GetName());
  }

 private:
  class FileHandle : public BlobFileHandle {
   public:
    FileHandle(uint64_t number, const std::string& name,
               std::unique_ptr<WritableFileWriter> file)
        : number_(number), name_(name), file_(std::move(file)) {}

    uint64_t GetNumber() const override { return number_; }

    const std::string& GetName() const override { return name_; }

    WritableFileWriter* GetFile() const override { return file_.get(); }

   private:
    friend class FileManager;

    uint64_t number_;
    std::string name_;
    std::unique_ptr<WritableFileWriter> file_;
  };

  Env* env_{Env::Default()};
  EnvOptions env_options_;
  TitanDBOptions db_options_;
  std::atomic<uint64_t> number_{0};
  VersionSet* vset_;
};

class TableBuilderTest : public testing::Test {
 public:
  TableBuilderTest()
      : cf_moptions_(cf_options_),
        cf_ioptions_(options_),
        tmpdir_(test::TmpDir(env_)),
        base_name_(tmpdir_ + "/base"),
        blob_name_(BlobFileName(tmpdir_, kTestFileNumber)) {
    db_options_.dirname = tmpdir_;
    cf_options_.min_blob_size = kMinBlobSize;
    vset_.reset(new VersionSet(db_options_, nullptr));
    std::map<uint32_t, TitanCFOptions> cfs{{0, cf_options_}};
    vset_->AddColumnFamilies(cfs);
    blob_manager_.reset(new FileManager(db_options_, vset_.get()));
    table_factory_.reset(new TitanTableFactory(db_options_, cf_options_,
                                               blob_manager_, &mutex_,
                                               vset_.get(), nullptr));
  }

  ~TableBuilderTest() {
    env_->DeleteFile(base_name_);
    env_->DeleteFile(blob_name_);
    env_->DeleteDir(tmpdir_);
  }

  void BlobFileExists(bool exists) {
    Status s = env_->FileExists(blob_name_);
    if (exists) {
      ASSERT_TRUE(s.ok());
    } else {
      ASSERT_TRUE(s.IsNotFound());
    }
  }

  void NewFileWriter(const std::string& fname,
                     std::unique_ptr<WritableFileWriter>* result) {
    std::unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(fname, &file, env_options_));
    result->reset(new WritableFileWriter(std::move(file), fname, env_options_));
  }

  void NewFileReader(const std::string& fname,
                     std::unique_ptr<RandomAccessFileReader>* result) {
    std::unique_ptr<RandomAccessFile> file;
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, env_options_));
    result->reset(new RandomAccessFileReader(std::move(file), fname, env_));
  }

  void NewBaseFileWriter(std::unique_ptr<WritableFileWriter>* result) {
    NewFileWriter(base_name_, result);
  }

  void NewBaseFileReader(std::unique_ptr<RandomAccessFileReader>* result) {
    NewFileReader(base_name_, result);
  }

  void NewBlobFileReader(std::unique_ptr<BlobFileReader>* result) {
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(blob_name_, &file);
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(blob_name_, &file_size));
    ASSERT_OK(BlobFileReader::Open(cf_options_, std::move(file), file_size,
                                   result, nullptr));
  }

  void NewTableReader(std::unique_ptr<TableReader>* result) {
    std::unique_ptr<RandomAccessFileReader> file;
    NewBaseFileReader(&file);
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file->file_name(), &file_size));
    TableReaderOptions options(cf_ioptions_, nullptr, env_options_,
                               cf_ioptions_.internal_comparator);
    ASSERT_OK(table_factory_->NewTableReader(options, std::move(file),
                                             file_size, result));
  }

  void NewTableBuilder(WritableFileWriter* file,
                       std::unique_ptr<TableBuilder>* result) {
    CompressionOptions compression_opts;
    TableBuilderOptions options(cf_ioptions_, cf_moptions_,
                                cf_ioptions_.internal_comparator, &collectors_,
                                kNoCompression, 0 /*sample_for_compression*/,
                                compression_opts, false /*skip_filters*/,
                                kDefaultColumnFamilyName, 0 /*level*/);
    result->reset(table_factory_->NewTableBuilder(options, 0, file));
  }

  void SetBuilderLevel(TitanTableBuilder* builder, int merge_level,
                       int target_level) {
    ASSERT_TRUE(builder != nullptr);
    builder->merge_level_ = merge_level;
    builder->target_level_ = target_level;
  }

  port::Mutex mutex_;

  Env* env_{Env::Default()};
  EnvOptions env_options_;
  Options options_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  MutableCFOptions cf_moptions_;
  ImmutableCFOptions cf_ioptions_;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>> collectors_;

  std::string tmpdir_;
  std::string base_name_;
  std::string blob_name_;
  std::unique_ptr<TableFactory> table_factory_;
  std::shared_ptr<BlobFileManager> blob_manager_;
  std::unique_ptr<VersionSet> vset_;
};

TEST_F(TableBuilderTest, Basic) {
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file.get(), &table_builder);

  // Build a base table and a blob file.
  const int n = 100;
  for (char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value;
    if (i % 2 == 0) {
      value = std::string(1, i);
    } else {
      value = std::string(kMinBlobSize, i);
    }
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());

  std::unique_ptr<TableReader> base_reader;
  NewTableReader(&base_reader);
  std::unique_ptr<BlobFileReader> blob_reader;
  NewBlobFileReader(&blob_reader);

  ReadOptions ro;
  std::unique_ptr<InternalIterator> iter;
  iter.reset(base_reader->NewIterator(ro, nullptr /*prefix_extractor*/,
                                      nullptr /*arena*/, false /*skip_filters*/,
                                      TableReaderCaller::kUncategorized));
  iter->SeekToFirst();
  for (char i = 0; i < n; i++) {
    ASSERT_TRUE(iter->Valid());
    std::string key(1, i);
    ParsedInternalKey ikey;
    ASSERT_TRUE(ParseInternalKey(iter->key(), &ikey));
    ASSERT_EQ(ikey.user_key, key);
    if (i % 2 == 0) {
      ASSERT_EQ(ikey.type, kTypeValue);
      ASSERT_EQ(iter->value(), std::string(1, i));
    } else {
      ASSERT_EQ(ikey.type, kTypeBlobIndex);
      BlobIndex index;
      ASSERT_OK(DecodeInto(iter->value(), &index));
      ASSERT_EQ(index.file_number, kTestFileNumber);
      BlobRecord record;
      PinnableSlice buffer;
      ASSERT_OK(blob_reader->Get(ro, index.blob_handle, &record, &buffer));
      ASSERT_EQ(record.key, key);
      ASSERT_EQ(record.value, std::string(kMinBlobSize, i));
    }
    iter->Next();
  }
}

TEST_F(TableBuilderTest, NoBlob) {
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file.get(), &table_builder);

  const int n = 100;
  for (char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value(1, i);
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());
  BlobFileExists(false);

  std::unique_ptr<TableReader> base_reader;
  NewTableReader(&base_reader);

  ReadOptions ro;
  std::unique_ptr<InternalIterator> iter;
  iter.reset(base_reader->NewIterator(ro, nullptr /*prefix_extractor*/,
                                      nullptr /*arena*/, false /*skip_filters*/,
                                      TableReaderCaller::kUncategorized));
  iter->SeekToFirst();
  for (char i = 0; i < n; i++) {
    ASSERT_TRUE(iter->Valid());
    std::string key(1, i);
    ParsedInternalKey ikey;
    ASSERT_TRUE(ParseInternalKey(iter->key(), &ikey));
    ASSERT_EQ(ikey.user_key, key);
    ASSERT_EQ(ikey.type, kTypeValue);
    ASSERT_EQ(iter->value(), std::string(1, i));
    iter->Next();
  }
}

TEST_F(TableBuilderTest, Abandon) {
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file.get(), &table_builder);

  const int n = 100;
  for (char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value;
    if (i % 2 == 0) {
      value = std::string(1, i);
    } else {
      value = std::string(kMinBlobSize, i);
    }
    table_builder->Add(ikey.Encode(), value);
  }
  BlobFileExists(true);
  table_builder->Abandon();
  BlobFileExists(false);
}

TEST_F(TableBuilderTest, NumEntries) {
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file.get(), &table_builder);

  // Build a base table and a blob file.
  const int n = 100;
  for (char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value;
    if (i % 2 == 0) {
      value = std::string(1, i);
    } else {
      value = std::string(kMinBlobSize, i);
    }
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_EQ(n, table_builder->NumEntries());
  ASSERT_OK(table_builder->Finish());
}

TEST_F(TableBuilderTest, TargeSize) {
  cf_options_.blob_file_target_size = kTargetBlobFileSize;
  table_factory_.reset(new TitanTableFactory(
      db_options_, cf_options_, blob_manager_, &mutex_, vset_.get(), nullptr));
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file.get(), &table_builder);
  const int n = 255;
  for (unsigned char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value(kMinBlobSize, i);
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_OK(table_builder->Finish());
  uint64_t file_size = 0;
  ASSERT_OK(env_->GetFileSize(blob_name_, &file_size));
  ASSERT_TRUE(file_size < kTargetBlobFileSize + 1 + kMinBlobSize);
  ASSERT_TRUE(file_size > kTargetBlobFileSize - 1 - kMinBlobSize);
}

TEST_F(TableBuilderTest, LevelMerge) {
  cf_options_.blob_file_target_size = kTargetBlobFileSize;
  cf_options_.level_merge = true;
  table_factory_.reset(new TitanTableFactory(
      db_options_, cf_options_, blob_manager_, &mutex_, vset_.get(), nullptr));
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file.get(), &table_builder);

  // generate a level 0 sst with blob file
  const int n = 255;
  for (unsigned char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value(kMinBlobSize, i);
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());

  std::unique_ptr<TableReader> base_reader;
  NewTableReader(&base_reader);

  std::string first_base_name = base_name_;
  base_name_ = base_name_ + "second";
  NewBaseFileWriter(&base_file);
  NewTableBuilder(base_file.get(), &table_builder);
  SetBuilderLevel(reinterpret_cast<TitanTableBuilder*>(table_builder.get()),
                  1 /* merge level */, 1 /* target level */);

  ReadOptions ro;
  std::unique_ptr<InternalIterator> first_iter;
  first_iter.reset(base_reader->NewIterator(
      ro, nullptr /*prefix_extractor*/, nullptr /*arena*/,
      false /*skip_filters*/, TableReaderCaller::kUncategorized));

  // compact level0 sst to level1, values will be merge to another blob file
  first_iter->SeekToFirst();
  for (unsigned char i = 0; i < n; i++) {
    ASSERT_TRUE(first_iter->Valid());
    table_builder->Add(first_iter->key(), first_iter->value());
    first_iter->Next();
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());

  std::unique_ptr<TableReader> second_base_reader;
  NewTableReader(&second_base_reader);
  std::unique_ptr<InternalIterator> second_iter;
  second_iter.reset(second_base_reader->NewIterator(
      ro, nullptr /*prefix_extractor*/, nullptr /*arena*/,
      false /*skip_filters*/, TableReaderCaller::kUncategorized));

  // make sure value address has changed after compaction
  first_iter->SeekToFirst();
  second_iter->SeekToFirst();
  for (unsigned char i = 0; i < n; i++) {
    ASSERT_TRUE(first_iter->Valid());
    ASSERT_TRUE(second_iter->Valid());
    ParsedInternalKey first_ikey, second_ikey;
    ASSERT_TRUE(ParseInternalKey(first_iter->key(), &first_ikey));
    ASSERT_TRUE(ParseInternalKey(first_iter->key(), &second_ikey));
    ASSERT_EQ(first_ikey.type, kTypeBlobIndex);
    ASSERT_EQ(second_ikey.type, kTypeBlobIndex);
    ASSERT_EQ(first_ikey.user_key, second_ikey.user_key);
    ASSERT_NE(first_iter->value(), second_iter->value());

    first_iter->Next();
    second_iter->Next();
  }

  env_->DeleteFile(first_base_name);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
