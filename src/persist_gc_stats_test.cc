#include <inttypes.h>
#include <options/cf_options.h>
#include <unordered_map>

#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "rocksdb/utilities/debug.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"

#include "blob_file_iterator.h"
#include "db_impl.h"
#include "titan/db.h"
#include "titan_fault_injection_test_env.h"

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

class PersistGCStatsTest : public testing::Test {
 public:
  PersistGCStatsTest() : dbname_(test::TmpDir()) {
    options_.dirname = dbname_ + "/titandb";
    options_.create_if_missing = true;
    options_.min_blob_size = 32;
    options_.min_gc_batch_size = 1;
    options_.merge_small_file_threshold = 0;
    options_.disable_background_gc = true;
    options_.blob_file_compression = CompressionType::kLZ4Compression;
    options_.persist_gc_stats = true;
    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
  }

  ~PersistGCStatsTest() {
    Close();
    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
  }

  void Open() {
    if (cf_names_.empty()) {
      ASSERT_OK(TitanDB::Open(options_, dbname_, &db_));
      db_impl_ = reinterpret_cast<TitanDBImpl*>(db_);
    } else {
      TitanDBOptions db_options(options_);
      TitanCFOptions cf_options(options_);
      cf_names_.clear();
      ASSERT_OK(DB::ListColumnFamilies(db_options, dbname_, &cf_names_));
      std::vector<TitanCFDescriptor> descs;
      for (auto& name : cf_names_) {
        descs.emplace_back(name, cf_options);
      }
      cf_handles_.clear();
      ASSERT_OK(TitanDB::Open(db_options, dbname_, descs, &cf_handles_, &db_));
      db_impl_ = reinterpret_cast<TitanDBImpl*>(db_);
    }
  }

  void Close() {
    if (!db_) return;
    for (auto& handle : cf_handles_) {
      db_->DestroyColumnFamilyHandle(handle);
    }
    ASSERT_OK(db_->Close());
    delete db_;
    db_ = nullptr;
  }

  void Reopen() {
    Close();
    Open();
  }

  void Put(uint64_t k, std::map<std::string, std::string>* data = nullptr) {
    WriteOptions wopts;
    std::string key = GenKey(k);
    std::string value = GenValue(k);
    ASSERT_OK(db_->Put(wopts, key, value));
    if (data != nullptr) {
      data->emplace(key, value);
    }
  }

  Status CheckGetGCStats(uint64_t blob_file_num) {
    char blob_file_num_str[16];
    sprintf(blob_file_num_str, "%015" PRIu64, blob_file_num);
    blob_file_num_str[15] = 0;
    PinnableSlice stats_value;
    Status s = db_impl_->db_impl_->Get(
        ReadOptions(), db_impl_->db_impl_->PersistentStatsColumnFamily(),
        Slice(blob_file_num_str), &stats_value);
    printf("%s\n", s.ToString().c_str());
    return s;
  }

  void Flush() {
    FlushOptions fopts;
    ASSERT_OK(db_->Flush(fopts));
  }

  std::weak_ptr<BlobStorage> GetBlobStorage(
      ColumnFamilyHandle* cf_handle = nullptr) {
    if (cf_handle == nullptr) {
      cf_handle = db_->DefaultColumnFamily();
    }
    MutexLock l(&db_impl_->mutex_);
    return db_impl_->blob_file_set_->GetBlobStorage(cf_handle->GetID());
  }

  void VerifyDB(const std::map<std::string, std::string>& data,
                ReadOptions ropts = ReadOptions()) {
    db_impl_->PurgeObsoleteFiles();

    for (auto& kv : data) {
      std::string value;
      ASSERT_OK(db_->Get(ropts, kv.first, &value));
      ASSERT_EQ(value, kv.second);
      for (auto& handle : cf_handles_) {
        ASSERT_OK(db_->Get(ropts, handle, kv.first, &value));
        ASSERT_EQ(value, kv.second);
      }
      std::vector<Slice> keys(cf_handles_.size(), kv.first);
      std::vector<std::string> values;
      auto res = db_->MultiGet(ropts, cf_handles_, keys, &values);
      for (auto& s : res) ASSERT_OK(s);
      for (auto& v : values) ASSERT_EQ(v, kv.second);
    }

    std::vector<Iterator*> iterators;
    db_->NewIterators(ropts, cf_handles_, &iterators);
    iterators.emplace_back(db_->NewIterator(ropts));
    for (auto& handle : cf_handles_) {
      iterators.emplace_back(db_->NewIterator(ropts, handle));
    }
    for (auto& iter : iterators) {
      iter->SeekToFirst();
      for (auto& kv : data) {
        ASSERT_EQ(iter->Valid(), true);
        ASSERT_EQ(iter->key(), kv.first);
        ASSERT_EQ(iter->value(), kv.second);
        iter->Next();
      }
      delete iter;
    }
  }

  void CompactAll() {
    auto opts = db_->GetOptions();
    auto compact_opts = CompactRangeOptions();
    compact_opts.change_level = true;
    compact_opts.target_level = opts.num_levels - 1;
    compact_opts.bottommost_level_compaction = BottommostLevelCompaction::kSkip;
    ASSERT_OK(db_->CompactRange(compact_opts, nullptr, nullptr));
  }

  std::string GenKey(uint64_t i) {
    char buf[64];
    snprintf(buf, sizeof(buf), "k-%08" PRIu64, i);
    return buf;
  }

  std::string GenValue(uint64_t k) {
    if (k % 2 == 0) {
      return std::string(options_.min_blob_size - 1, 'v');
    } else {
      return std::string(options_.min_blob_size + 1, 'v');
    }
  }

  void SetBGError(const Status& s) {
    MutexLock l(&db_impl_->mutex_);
    db_impl_->SetBGError(s);
  }

  void CallGC() {
    db_impl_->bg_gc_scheduled_++;
    db_impl_->BackgroundCallGC();
    while (db_impl_->bg_gc_scheduled_)
      ;
  }

  // Make db ignore first bg_error
  class BGErrorListener : public EventListener {
   public:
    void OnBackgroundError(BackgroundErrorReason reason,
                           Status* error) override {
      if (++cnt == 1) *error = Status();
    }

   private:
    int cnt{0};
  };

  Env* env_{Env::Default()};
  std::string dbname_;
  TitanOptions options_;
  TitanDB* db_{nullptr};
  TitanDBImpl* db_impl_{nullptr};
  std::vector<std::string> cf_names_;
  std::vector<ColumnFamilyHandle*> cf_handles_;
};

TEST_F(PersistGCStatsTest, NewGCStats) {
  const uint64_t kNumKeys = 100;
  Open();
  std::map<std::string, std::string> data;
  for (uint64_t k = 1; k <= kNumKeys; k++) {
    Put(k, &data);
  }
  Flush();
  auto bs = GetBlobStorage(db_->DefaultColumnFamily()).lock();
  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> files;
  bs->ExportBlobFiles(files);
  for (auto file : files) {
    ASSERT_OK(CheckGetGCStats(file.first));
  }
}
}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
