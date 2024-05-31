#include "table_builder.h"

#include "file/filename.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "test_util/testharness.h"

#include "blob_file_manager.h"
#include "blob_file_reader.h"
#include "blob_file_set.h"
#include "db_impl.h"
#include "table_factory.h"

namespace rocksdb {
namespace titandb {

const uint64_t kMinBlobSize = 128;
const uint64_t kTestFileNumber = 123;
const uint64_t kTargetBlobFileSize = 4096;

class FileManager : public BlobFileManager {
 public:
  FileManager(const TitanDBOptions& db_options, BlobFileSet* blob_file_set)
      : db_options_(db_options),
        number_(kTestFileNumber),
        blob_file_set_(blob_file_set) {}

  Status NewFile(std::unique_ptr<BlobFileHandle>* handle,
                 Env::IOPriority pri = Env::IOPriority::IO_TOTAL) override {
    auto number = number_.fetch_add(1);
    auto name = BlobFileName(db_options_.dirname, number);
    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<FSWritableFile> f;
      Status s = env_->GetFileSystem()->NewWritableFile(
          name, FileOptions(env_options_), &f, nullptr /*dbg*/);
      if (!s.ok()) return s;
      f->SetIOPriority(pri);
      file.reset(new WritableFileWriter(std::move(f), name,
                                        FileOptions(env_options_)));
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
      auto storage = blob_file_set_->GetBlobStorage(0).lock();
      storage->AddBlobFile(file);
    }
    return s;
  }

  Status BatchFinishFiles(
      uint32_t cf_id,
      const std::vector<std::pair<std::shared_ptr<BlobFileMeta>,
                                  std::unique_ptr<BlobFileHandle>>>& files)
      override {
    Status s;
    for (auto& file : files) {
      s = FinishFile(cf_id, file.first,
                     const_cast<std::unique_ptr<BlobFileHandle>&&>(
                         std::move(file.second)));
      if (!s.ok()) return s;
    }
    return s;
  }

  Status DeleteFile(std::unique_ptr<BlobFileHandle>&& handle) override {
    return env_->DeleteFile(handle->GetName());
  }

  uint64_t LastBlobNumber() { return number_.load() - 1; }

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
  BlobFileSet* blob_file_set_;
};

class TestTableFactory : public TableFactory {
 private:
  std::shared_ptr<TableFactory> base_factory_;
  mutable TableBuilder* latest_table_builder_ = nullptr;

 public:
  TestTableFactory(std::shared_ptr<TableFactory>& base_factory)
      : base_factory_(base_factory) {}

  const char* Name() const override { return "TestTableFactory"; }

  using TableFactory::NewTableReader;

  Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* result,
      bool prefetch_index_and_filter_in_cache) const override {
    return base_factory_->NewTableReader(ro, options, std::move(file),
                                         file_size, result,
                                         prefetch_index_and_filter_in_cache);
  }

  TableBuilder* NewTableBuilder(const TableBuilderOptions& options,
                                WritableFileWriter* file) const override {
    latest_table_builder_ = base_factory_->NewTableBuilder(options, file);
    return latest_table_builder_;
  }

  bool IsDeleteRangeSupported() const override {
    return base_factory_->IsDeleteRangeSupported();
  }

  TableBuilder* latest_table_builder() const { return latest_table_builder_; }
};

class TableBuilderTest : public testing::Test {
 public:
  TableBuilderTest()
      : tmpdir_(test::TmpDir(env_)),
        base_file_number_(1),
        blob_name_(BlobFileName(tmpdir_, kTestFileNumber)) {
    db_options_.dirname = tmpdir_;
    db_options_.statistics = nullptr;
    cf_options_.min_blob_size = kMinBlobSize;
    Open();
  }

  ~TableBuilderTest() {
    uint64_t last_blob_number =
        reinterpret_cast<FileManager*>(blob_manager_.get())->LastBlobNumber();
    for (uint64_t i = kTestFileNumber; i <= last_blob_number; i++) {
      env_->DeleteFile(BlobFileName(tmpdir_, i));
    }
    env_->DeleteFile(FileNumberToName(base_file_number_));
    env_->DeleteDir(tmpdir_);
  }

  void Open() {
    // Refresh options.
    db_ioptions_ = ImmutableDBOptions(db_options_);
    cf_moptions_ = MutableCFOptions(cf_options_);
    cf_ioptions_ = ImmutableCFOptions(cf_options_);
    ioptions_ = ImmutableOptions(db_ioptions_, cf_ioptions_);
    db_impl_.reset(new TitanDBImpl(db_options_, tmpdir_));
    blob_file_set_.reset(
        new BlobFileSet(db_options_, nullptr, nullptr, &db_impl_->mutex_));
    std::map<uint32_t, TitanCFOptions> cfs{{0, cf_options_}};
    db_impl_->TEST_set_initialized(true);
    blob_file_set_->Open(cfs, "");
    blob_manager_.reset(new FileManager(db_options_, blob_file_set_.get()));
    // Replace base table facotry.
    base_table_factory_ =
        std::make_shared<TestTableFactory>(cf_options_.table_factory);
    cf_options_.table_factory = base_table_factory_;
    cf_ioptions_.table_factory = base_table_factory_;
    table_factory_.reset(new TitanTableFactory(db_options_, cf_options_,
                                               blob_manager_, &mutex_,
                                               blob_file_set_.get(), nullptr));
  }

  void BlobFileExists(bool exists) {
    Status s = env_->FileExists(blob_name_);
    if (exists) {
      ASSERT_TRUE(s.ok());
    } else {
      ASSERT_TRUE(s.IsNotFound());
    }
  }

  std::string FileNumberToName(const uint64_t file_number) {
    return tmpdir_ + "/" + std::to_string(file_number);
  }

  void NewFileWriter(const std::string& fname,
                     std::unique_ptr<WritableFileWriter>* result) {
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(env_->GetFileSystem()->NewWritableFile(
        fname, FileOptions(env_options_), &file, nullptr /*dbg*/));
    result->reset(new WritableFileWriter(std::move(file), fname,
                                         FileOptions(env_options_)));
  }

  void NewFileReader(const std::string& fname,
                     std::unique_ptr<RandomAccessFileReader>* result) {
    std::unique_ptr<FSRandomAccessFile> file;
    ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(
        fname, FileOptions(env_options_), &file, nullptr /*dbg*/));
    result->reset(new RandomAccessFileReader(std::move(file), fname,
                                             env_->GetSystemClock().get()));
  }

  void NewBaseFileWriter(std::unique_ptr<WritableFileWriter>* result) {
    NewFileWriter(FileNumberToName(base_file_number_), result);
  }

  void NewBaseFileReader(std::unique_ptr<RandomAccessFileReader>* result) {
    NewFileReader(FileNumberToName(base_file_number_), result);
  }

  void NewBlobFileReader(std::unique_ptr<BlobFileReader>* result) {
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(blob_name_, &file);
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(blob_name_, &file_size));
    ASSERT_OK(BlobFileReader::Open(cf_options_, std::move(file), file_size,
                                   result, nullptr));
  }

  void NewTableReader(const uint64_t file_number,
                      std::unique_ptr<TableReader>* result) {
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(FileNumberToName(file_number), &file);
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file->file_name(), &file_size));
    TableReaderOptions options(ioptions_, prefix_extractor_, env_options_,
                               cf_ioptions_.internal_comparator, 0);
    options.cur_file_num = file_number;
    ASSERT_OK(table_factory_->NewTableReader(options, std::move(file),
                                             file_size, result));
  }

  void NewTableBuilder(const uint64_t file_number, WritableFileWriter* file,
                       std::unique_ptr<TableBuilder>* result,
                       int target_level = 0) {
    CompressionOptions compression_opts;
    NewTableBuilder(file_number, file, result, compression_opts, target_level);
  }

  void NewTableBuilder(const uint64_t file_number, WritableFileWriter* file,
                       std::unique_ptr<TableBuilder>* result,
                       CompressionOptions compression_opts,
                       int target_level = 0) {
    TableBuilderOptions options(
        ioptions_, cf_moptions_, cf_ioptions_.internal_comparator, &collectors_,
        kNoCompression, compression_opts, 0 /*column_family_id*/,
        kDefaultColumnFamilyName, target_level, false,
        TableFileCreationReason::kMisc, 0, 0, "", "", 0, file_number);
    result->reset(table_factory_->NewTableBuilder(options, file));
  }

  port::Mutex mutex_;

  Env* env_{Env::Default()};
  EnvOptions env_options_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>> collectors_;
  // Derived options.
  ImmutableDBOptions db_ioptions_;
  MutableCFOptions cf_moptions_;
  ImmutableCFOptions cf_ioptions_;
  ImmutableOptions ioptions_;
  std::shared_ptr<const SliceTransform> prefix_extractor_ = nullptr;

  std::string tmpdir_;
  uint64_t base_file_number_;
  std::string blob_name_;
  std::unique_ptr<TitanDBImpl> db_impl_;
  std::shared_ptr<TestTableFactory> base_table_factory_;
  std::unique_ptr<TableFactory> table_factory_;
  std::shared_ptr<BlobFileManager> blob_manager_;
  std::unique_ptr<BlobFileSet> blob_file_set_;
};

TEST_F(TableBuilderTest, Basic) {
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file_number_, base_file.get(), &table_builder);

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
  NewTableReader(base_file_number_, &base_reader);
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
    ASSERT_OK(ParseInternalKey(iter->key(), &ikey, false));
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
      OwnedSlice buffer;
      ASSERT_OK(blob_reader->Get(ro, index.blob_handle, &record, &buffer));
      ASSERT_EQ(record.key, key);
      ASSERT_EQ(record.value, std::string(kMinBlobSize, i));
    }
    iter->Next();
  }
}

TEST_F(TableBuilderTest, DictCompress) {
#if ZSTD_VERSION_NUMBER >= 10103
  CompressionOptions compression_opts;
  compression_opts.enabled = true;
  compression_opts.max_dict_bytes = 4000;
  cf_options_.blob_file_compression_options = compression_opts;
  cf_options_.blob_file_compression = kZSTD;

  table_factory_.reset(new TitanTableFactory(db_options_, cf_options_,
                                             blob_manager_, &mutex_,
                                             blob_file_set_.get(), nullptr));

  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file_number_, base_file.get(), &table_builder);

  // Build a base table and a blob file.
  const int n = 100;
  for (char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value;
    value = std::string(kMinBlobSize, i);
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());
  BlobFileExists(true);

  std::unique_ptr<TableReader> base_reader;
  NewTableReader(base_file_number_, &base_reader);
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
    ASSERT_OK(ParseInternalKey(iter->key(), &ikey, false));
    ASSERT_EQ(ikey.user_key, key);
    ASSERT_EQ(ikey.type, kTypeBlobIndex);
    BlobIndex index;
    ASSERT_OK(DecodeInto(iter->value(), &index));
    ASSERT_EQ(index.file_number, kTestFileNumber);
    BlobRecord record;
    OwnedSlice buffer;
    ASSERT_OK(blob_reader->Get(ro, index.blob_handle, &record, &buffer));
    ASSERT_EQ(record.key, key);
    ASSERT_EQ(record.value, std::string(kMinBlobSize, i));
    iter->Next();
  }
  ASSERT_TRUE(!iter->Valid());
#endif
}

TEST_F(TableBuilderTest, DictCompressOptions) {
#if ZSTD_VERSION_NUMBER >= 10103
  CompressionOptions compression_opts;
  compression_opts.window_bits = -14;
  compression_opts.level = 32767;
  compression_opts.strategy = 0;
  compression_opts.max_dict_bytes = 4000;
  compression_opts.zstd_max_train_bytes = 0;

  compression_opts.enabled = true;
  compression_opts.max_dict_bytes = 4000;
  cf_options_.blob_file_compression_options = compression_opts;
  cf_options_.blob_file_compression = kZSTD;

  table_factory_.reset(new TitanTableFactory(db_options_, cf_options_,
                                             blob_manager_, &mutex_,
                                             blob_file_set_.get(), nullptr));

  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file_number_, base_file.get(), &table_builder);

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
  ASSERT_EQ(n / 2, table_builder->NumEntries());
  ASSERT_OK(table_builder->Finish());
#endif
}

TEST_F(TableBuilderTest, DictCompressDisorder) {
#if ZSTD_VERSION_NUMBER >= 10103
  CompressionOptions compression_opts;
  compression_opts.enabled = true;
  compression_opts.max_dict_bytes = 4000;
  cf_options_.blob_file_compression_options = compression_opts;
  cf_options_.blob_file_compression = kZSTD;

  table_factory_.reset(new TitanTableFactory(db_options_, cf_options_,
                                             blob_manager_, &mutex_,
                                             blob_file_set_.get(), nullptr));

  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file_number_, base_file.get(), &table_builder);

  // Build a base table and a blob file.
  const int n = 100;
  for (char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value;
    if (i % 3 == 0) {
      value = std::string(1, i);
    } else if (i % 3 == 1) {
      value = std::string(kMinBlobSize, i);
    } else if (i % 3 == 2) {
      ikey.Set(key, 1, kTypeBlobIndex);
      BlobIndex blobIndex;
      // set different values in different fields
      blobIndex.file_number = i;
      blobIndex.blob_handle.size = i * 2 + 1;
      blobIndex.blob_handle.offset = i * 3 + 2;
      blobIndex.EncodeTo(&value);
    }
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());
  std::unique_ptr<TableReader> base_reader;
  NewTableReader(base_file_number_, &base_reader);
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
    ASSERT_OK(ParseInternalKey(iter->key(), &ikey, false));
    // check order
    ASSERT_EQ(ikey.user_key, key);
    if (i % 3 == 0) {
      ASSERT_EQ(ikey.type, kTypeValue);
      ASSERT_EQ(iter->value(), std::string(1, i));
    } else if (i % 3 == 1) {
      ASSERT_EQ(ikey.type, kTypeBlobIndex);
      BlobIndex index;
      ASSERT_OK(DecodeInto(iter->value(), &index));
      ASSERT_EQ(index.file_number, kTestFileNumber);
      BlobRecord record;
      OwnedSlice buffer;
      ASSERT_OK(blob_reader->Get(ro, index.blob_handle, &record, &buffer));
      ASSERT_EQ(record.key, key);
      ASSERT_EQ(record.value, std::string(kMinBlobSize, i));
    } else if (i % 3 == 2) {
      ASSERT_EQ(ikey.type, kTypeBlobIndex);
      BlobIndex index;
      // We do not have corresponding blob file in this test, so we only check
      // BlobIndex.
      ASSERT_OK(DecodeInto(iter->value(), &index));
      ASSERT_EQ(index.file_number, i);
      ASSERT_EQ(index.blob_handle.size, i * 2 + 1);
      ASSERT_EQ(index.blob_handle.offset, i * 3 + 2);
    }
    iter->Next();
  }
#endif
}

TEST_F(TableBuilderTest, NoBlob) {
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file_number_, base_file.get(), &table_builder);

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
  NewTableReader(base_file_number_, &base_reader);

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
    ASSERT_OK(ParseInternalKey(iter->key(), &ikey, false));
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
  NewTableBuilder(base_file_number_, base_file.get(), &table_builder);

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
  NewTableBuilder(base_file_number_, base_file.get(), &table_builder);

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

// To test size of each blob file is around blob_file_target_size after building
TEST_F(TableBuilderTest, TargetSize) {
  cf_options_.blob_file_target_size = kTargetBlobFileSize;
  table_factory_.reset(new TitanTableFactory(db_options_, cf_options_,
                                             blob_manager_, &mutex_,
                                             blob_file_set_.get(), nullptr));
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file_number_, base_file.get(), &table_builder);
  const int n = 255;
  for (unsigned char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value(kMinBlobSize, i);
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_OK(table_builder->Finish());
  uint64_t last_file_number =
      reinterpret_cast<FileManager*>(blob_manager_.get())->LastBlobNumber();
  for (uint64_t i = kTestFileNumber; i <= last_file_number; i++) {
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(BlobFileName(tmpdir_, i), &file_size));
    ASSERT_TRUE(file_size < kTargetBlobFileSize + 1 + kMinBlobSize);
    if (i < last_file_number) {
      ASSERT_TRUE(file_size > kTargetBlobFileSize - 1 - kMinBlobSize);
    }
  }
}

// Compact a level 0 file to last level, to test level merge is functional and
// correct
TEST_F(TableBuilderTest, LevelMerge) {
  cf_options_.level_merge = true;
  Open();
  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file_number_, base_file.get(), &table_builder,
                  0 /* target_level */);

  // Generate a level 0 sst with blob file
  const int n = 1;
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
  NewTableReader(base_file_number_, &base_reader);
  ReadOptions ro;
  std::unique_ptr<InternalIterator> first_iter;
  first_iter.reset(base_reader->NewIterator(
      ro, nullptr /*prefix_extractor*/, nullptr /*arena*/,
      false /*skip_filters*/, TableReaderCaller::kUncategorized));

  // Base file of last level sst
  auto second_base = base_file_number_ + 1;
  NewFileWriter(FileNumberToName(second_base), &base_file);
  NewTableBuilder(second_base, base_file.get(), &table_builder,
                  cf_options_.num_levels - 1);

  first_iter->SeekToFirst();
  // Compact level0 sst to last level, values will be merge to another blob file
  for (unsigned char i = 0; i < n; i++) {
    ASSERT_TRUE(first_iter->Valid());
    table_builder->Add(first_iter->key(), first_iter->value());
    first_iter->Next();
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());

  std::unique_ptr<TableReader> second_base_reader;
  NewTableReader(second_base, &second_base_reader);
  std::unique_ptr<InternalIterator> second_iter;
  second_iter.reset(second_base_reader->NewIterator(
      ro, nullptr /*prefix_extractor*/, nullptr /*arena*/,
      false /*skip_filters*/, TableReaderCaller::kUncategorized));

  // Compare key, index and blob records after level merge
  first_iter->SeekToFirst();
  second_iter->SeekToFirst();
  auto storage = blob_file_set_->GetBlobStorage(0).lock();
  for (unsigned char i = 0; i < n; i++) {
    ASSERT_TRUE(first_iter->Valid());
    ASSERT_TRUE(second_iter->Valid());

    // Compare sst key
    ParsedInternalKey first_ikey, second_ikey;
    ASSERT_OK(ParseInternalKey(first_iter->key(), &first_ikey, false));
    ASSERT_OK(ParseInternalKey(second_iter->key(), &second_ikey, false));
    ASSERT_EQ(first_ikey.type, kTypeBlobIndex);
    ASSERT_EQ(second_ikey.type, kTypeBlobIndex);
    ASSERT_EQ(first_ikey.user_key, second_ikey.user_key);

    // Compare blob records
    Slice first_value = first_iter->value();
    Slice second_value = second_iter->value();
    BlobIndex first_index, second_index;
    BlobRecord first_record, second_record;
    PinnableSlice first_buffer, second_buffer;
    ASSERT_OK(first_index.DecodeFrom(&first_value));
    ASSERT_OK(second_index.DecodeFrom(&second_value));
    ASSERT_FALSE(first_index == second_index);
    ASSERT_OK(
        storage->Get(ReadOptions(), first_index, &first_record, &first_buffer));
    ASSERT_OK(storage->Get(ReadOptions(), second_index, &second_record,
                           &second_buffer));
    ASSERT_EQ(first_record.key, second_record.key);
    ASSERT_EQ(first_record.value, second_record.value);

    first_iter->Next();
    second_iter->Next();
  }

  env_->DeleteFile(FileNumberToName(second_base));
}

// Write blob index, to test key order is correct with dictionary compression
TEST_F(TableBuilderTest, LevelMergeWithDictCompressDisorder) {
#if ZSTD_VERSION_NUMBER >= 10103
  cf_options_.level_merge = true;
  table_factory_.reset(new TitanTableFactory(db_options_, cf_options_,
                                             blob_manager_, &mutex_,
                                             blob_file_set_.get(), nullptr));

  std::unique_ptr<WritableFileWriter> base_file;
  NewBaseFileWriter(&base_file);
  std::unique_ptr<TableBuilder> table_builder;
  NewTableBuilder(base_file_number_, base_file.get(), &table_builder);

  // Generate level 0 sst with blob file
  int n = 100;
  for (unsigned char i = 0; i < n; i++) {
    if (i % 2 == 0) {
      // key: 0, 2, 4, 6 ..98
      std::string key(1, i);
      InternalKey ikey(key, 1, kTypeValue);
      std::string value(kMinBlobSize, i);
      table_builder->Add(ikey.Encode(), value);
    } else {
      // key: 1, 3, 5, ..99
      std::string key(1, i);
      InternalKey ikey(key, 1, kTypeValue);
      std::string value = std::string(1, i);
      table_builder->Add(ikey.Encode(), value);
    }
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());

  std::unique_ptr<TableReader> base_reader;
  NewTableReader(base_file_number_, &base_reader);
  ReadOptions ro;
  std::unique_ptr<InternalIterator> first_iter;
  first_iter.reset(base_reader->NewIterator(
      ro, nullptr /*prefix_extractor*/, nullptr /*arena*/,
      false /*skip_filters*/, TableReaderCaller::kUncategorized));

  // Base file of last level sst
  auto second_base = base_file_number_ + 1;
  NewFileWriter(FileNumberToName(second_base), &base_file);

  CompressionOptions compression_opts;
  compression_opts.enabled = true;
  compression_opts.max_dict_bytes = 4000;
  cf_options_.blob_file_compression_options = compression_opts;
  cf_options_.blob_file_compression = kZSTD;

  NewTableBuilder(base_file_number_, base_file.get(), &table_builder,
                  compression_opts, cf_options_.num_levels - 1);

  first_iter->SeekToFirst();
  // compact data from level0 to level1
  for (unsigned char i = 0; i < n; i++) {
    ASSERT_TRUE(first_iter->Valid());
    table_builder->Add(first_iter->key(), first_iter->value());
    first_iter->Next();
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());

  std::unique_ptr<TableReader> second_base_reader;
  NewTableReader(second_base, &second_base_reader);
  std::unique_ptr<InternalIterator> second_iter;
  second_iter.reset(second_base_reader->NewIterator(
      ro, nullptr /*prefix_extractor*/, nullptr /*arena*/,
      false /*skip_filters*/, TableReaderCaller::kUncategorized));

  std::unique_ptr<BlobFileReader> blob_reader;
  NewBlobFileReader(&blob_reader);

  first_iter->SeekToFirst();
  second_iter->SeekToFirst();
  // check orders of keys
  for (unsigned char i = 0; i < n; i++) {
    ASSERT_TRUE(second_iter->Valid());

    ASSERT_TRUE(first_iter->Valid());

    // Compare sst key
    ParsedInternalKey first_ikey, second_ikey;
    ASSERT_OK(ParseInternalKey(first_iter->key(), &first_ikey, false));
    ASSERT_OK(ParseInternalKey(first_iter->key(), &second_ikey, false));
    ASSERT_EQ(first_ikey.type, second_ikey.type);
    ASSERT_EQ(first_ikey.user_key, second_ikey.user_key);

    if (i % 2 == 0) {
      // key: 0, 2, 4, 6 ..98
      ASSERT_EQ(second_ikey.type, kTypeBlobIndex);
      std::string key(1, i);

      BlobIndex index;
      ASSERT_OK(DecodeInto(second_iter->value(), &index));
      BlobRecord record;
      OwnedSlice buffer;
      ASSERT_OK(blob_reader->Get(ro, index.blob_handle, &record, &buffer));
      ASSERT_EQ(record.key, key);
      ASSERT_EQ(record.value, std::string(kMinBlobSize, i));
    } else {
      // key: 1, 3, 5, ..99
      std::string key(1, i);
      std::string value = std::string(1, i);

      ASSERT_EQ(second_ikey.type, kTypeValue);
      ASSERT_EQ(second_iter->value(), value);
    }

    first_iter->Next();
    second_iter->Next();
  }

  env_->DeleteFile(FileNumberToName(second_base));
#endif
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
