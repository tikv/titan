#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/random.h"
#include "util/string_util.h"

#include "blob_file_set.h"
#include "blob_format.h"
#include "blob_storage.h"
#include "db_impl.h"

namespace rocksdb {
namespace titandb {

void DeleteDir(Env* env, const std::string& dirname) {
  std::vector<std::string> filenames;
  env->GetChildren(dirname, &filenames);
  for (auto& fname : filenames) {
    env->DeleteFile(dirname + "/" + fname);
  }
  env->DeleteDir(dirname);
}

class TitanGCStatsTest : public testing::Test {
 public:
  TitanGCStatsTest() : dbname_(test::TmpDir()) {
    options_.disable_auto_compactions = true;
    options_.level_compaction_dynamic_level_bytes = true;

    options_.dirname = dbname_ + "/titandb";
    options_.create_if_missing = true;
    options_.min_blob_size = 0;
    options_.merge_small_file_threshold = 0;
    options_.min_gc_batch_size = 0;
    options_.disable_background_gc = true;
    options_.blob_file_compression = CompressionType::kNoCompression;

    // Clear directory.
    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
  }

  ~TitanGCStatsTest() {
    Close();
    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  Status Open() {
    Status s = TitanDB::Open(options_, dbname_, &db_);
    db_impl_ = reinterpret_cast<TitanDBImpl*>(db_);
    return s;
  }

  Status Close() {
    if (db_ == nullptr) {
      return Status::OK();
    }
    Status s = db_->Close();
    delete db_;
    db_ = db_impl_ = nullptr;
    return s;
  }

  Status Reopen() {
    Status s = Close();
    if (s.ok()) {
      s = Open();
    }
    return s;
  }

  Status KeyExists(uint32_t key, bool* exists) {
    PinnableSlice value;
    Status s = db_->Get(ReadOptions(), db_->DefaultColumnFamily(), gen_key(key),
                        &value);
    if (s.ok()) {
      *exists = true;
    } else if (s.IsNotFound()) {
      *exists = false;
      s = Status::OK();
    }
    return s;
  }

  Status Put(uint32_t key, const Slice& value) {
    return db_->Put(WriteOptions(), gen_key(key), value);
  }

  Status Delete(uint32_t key) {
    return db_->Delete(WriteOptions(), gen_key(key));
  }

  Status Flush() { return db_->Flush(FlushOptions()); }

  Status CompactAll() {
    return db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  }

  Status DeleteFilesInRange(const std::string& begin, const std::string& end) {
    Slice begin_slice(begin);
    Slice end_slice(end);
    RangePtr range(&begin_slice, &end_slice);
    return db_->DeleteFilesInRanges(db_->DefaultColumnFamily(), &range, 1);
  }

  std::string gen_key(uint32_t key) const {
    char buf[kKeySize + 1];
    sprintf(buf, "%010u", key);
    return std::string(buf);
  }

  uint64_t get_blob_size(const Slice& value) const {
    BlobRecord record;
    std::string key_str = gen_key(0);
    record.key = Slice(key_str);
    record.value = value;
    BlobEncoder encoder(options_.blob_file_compression);
    encoder.EncodeRecord(record);
    return static_cast<uint64_t>(encoder.GetEncodedSize());
  }

  void GetBlobFiles(
      std::map<uint64_t, std::weak_ptr<BlobFileMeta>>* blob_files) {
    assert(blob_files != nullptr);
    std::shared_ptr<BlobStorage> blob_storage =
        db_impl_->TEST_GetBlobStorage(db_->DefaultColumnFamily());
    blob_storage->ExportBlobFiles(*blob_files);
  }

 protected:
  static constexpr size_t kKeySize = 10;

  Env* env_ = Env::Default();
  std::string dbname_;
  TitanOptions options_;
  TitanDB* db_ = nullptr;
  TitanDBImpl* db_impl_ = nullptr;
};

TEST_F(TitanGCStatsTest, DeleteFilesInRange) {
  constexpr size_t kValueSize = 123;
  constexpr size_t kNumKeys = 10;
  std::string value(kValueSize, 'v');
  uint64_t blob_size = get_blob_size(value);
  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
  std::string num_sst;

  // Generate a blob file.
  ASSERT_OK(Open());
  for (uint32_t k = 1; k <= kNumKeys; k++) {
    ASSERT_OK(Put(k, value));
  }
  ASSERT_OK(Flush());
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &num_sst));
  ASSERT_EQ("1", num_sst);
  GetBlobFiles(&blob_files);
  ASSERT_EQ(1, blob_files.size());
  std::shared_ptr<BlobFileMeta> blob_file = blob_files.begin()->second.lock();
  ASSERT_EQ(0, blob_file->discardable_size());

  // Force to split SST into smaller ones. With the current rocksdb
  // implementation it split the file into every two keys per SST.
  options_.min_blob_size = 1000;
  options_.target_file_size_base = 1;
  BlockBasedTableOptions table_opts;
  table_opts.block_size = 1;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_opts));
  ASSERT_OK(Reopen());

  // Verify GC stats after reopen.
  GetBlobFiles(&blob_files);
  ASSERT_EQ(1, blob_files.size());
  blob_file = blob_files.begin()->second.lock();
  ASSERT_TRUE(blob_file != nullptr);
  ASSERT_EQ(0, blob_file->discardable_size());

  // Add a overlapping SST to disable trivial move.
  ASSERT_OK(Put(0, value));
  ASSERT_OK(Put(kNumKeys + 1, value));
  ASSERT_OK(Flush());

  // Compact to split SST.
  ASSERT_OK(CompactAll());
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level6", &num_sst));
  ASSERT_EQ("6", num_sst);
  GetBlobFiles(&blob_files);
  ASSERT_EQ(1, blob_files.size());
  blob_file = blob_files.begin()->second.lock();
  ASSERT_TRUE(blob_file != nullptr);
  ASSERT_EQ(0, blob_file->discardable_size());

  // Check live data size updated after DeleteFilesInRange.
  std::string key4 = gen_key(4);
  std::string key7 = gen_key(7);
  ASSERT_OK(DeleteFilesInRange(key4, key7));
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level6", &num_sst));
  ASSERT_EQ("4", num_sst);
  GetBlobFiles(&blob_files);
  ASSERT_EQ(1, blob_files.size());
  blob_file = blob_files.begin()->second.lock();
  ASSERT_TRUE(blob_file != nullptr);
  ASSERT_EQ(blob_size * 4, blob_file->discardable_size());
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
