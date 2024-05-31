#include <cinttypes>
#include <unordered_map>

#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "monitoring/statistics_impl.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/utilities/debug.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"

#include "blob_file_iterator.h"
#include "blob_file_reader.h"
#include "blob_file_size_collector.h"
#include "db_impl.h"
#include "db_iter.h"
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

class TitanDBTest : public testing::Test {
 public:
  TitanDBTest() : dbname_(test::TmpDir()) {
    options_.dirname = dbname_ + "/titandb";
    options_.create_if_missing = true;
    options_.min_blob_size = 32;
    options_.min_gc_batch_size = 1;
    options_.disable_background_gc = true;
    options_.disable_auto_compactions = true;
    options_.blob_file_compression = CompressionType::kLZ4Compression;
    options_.statistics = CreateDBStatistics();
    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
  }

  ~TitanDBTest() {
    Close();
    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
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

  void WaitGCInitialization() { db_impl_->thread_initialize_gc_->join(); }

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
    WaitGCInitialization();
  }

  void AddCF(const std::string& name) {
    TitanCFDescriptor desc(name, options_);
    ColumnFamilyHandle* handle = nullptr;
    ASSERT_OK(db_->CreateColumnFamily(desc, &handle));
    cf_names_.emplace_back(name);
    cf_handles_.emplace_back(handle);
  }

  void DropCF(const std::string& name) {
    for (size_t i = 0; i < cf_names_.size(); i++) {
      if (cf_names_[i] != name) continue;
      auto handle = cf_handles_[i];
      ASSERT_OK(db_->DropColumnFamily(handle));
      db_->DestroyColumnFamilyHandle(handle);
      cf_names_.erase(cf_names_.begin() + i);
      cf_handles_.erase(cf_handles_.begin() + i);
      break;
    }
  }

  Status LogAndApply(VersionEdit& edit) {
    MutexLock l(&db_impl_->mutex_);
    return db_impl_->blob_file_set_->LogAndApply(edit);
  }

  void Put(uint64_t k, std::map<std::string, std::string>* data = nullptr) {
    WriteOptions wopts;
    std::string key = GenKey(k);
    std::string value = GenValue(k);
    ASSERT_OK(db_->Put(wopts, key, value));
    for (auto& handle : cf_handles_) {
      ASSERT_OK(db_->Put(wopts, handle, key, value));
    }
    if (data != nullptr) {
      data->emplace(key, value);
    }
  }

  void Delete(uint64_t k) {
    WriteOptions wopts;
    std::string key = GenKey(k);
    ASSERT_OK(db_->Delete(wopts, key));
    for (auto& handle : cf_handles_) {
      ASSERT_OK(db_->Delete(wopts, handle, key));
    }
  }

  void Flush(ColumnFamilyHandle* cf_handle = nullptr) {
    if (cf_handle == nullptr) {
      cf_handle = db_->DefaultColumnFamily();
    }
    FlushOptions fopts;
    fopts.wait = true;
    ASSERT_OK(db_->Flush(fopts));
    for (auto& handle : cf_handles_) {
      ASSERT_OK(db_->Flush(fopts, handle));
    }
  }

  bool GetIntProperty(const Slice& property, uint64_t* value) {
    return db_->GetIntProperty(property, value);
  }

  std::weak_ptr<BlobStorage> GetBlobStorage(
      ColumnFamilyHandle* cf_handle = nullptr) {
    if (cf_handle == nullptr) {
      cf_handle = db_->DefaultColumnFamily();
    }
    MutexLock l(&db_impl_->mutex_);
    return db_impl_->blob_file_set_->GetBlobStorage(cf_handle->GetID());
  }

  void CheckBlobFileCount(int count, ColumnFamilyHandle* cf_handle = nullptr) {
    db_impl_->TEST_WaitForBackgroundGC();
    ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
    std::shared_ptr<BlobStorage> blob_storage =
        GetBlobStorage(cf_handle).lock();
    ASSERT_TRUE(blob_storage != nullptr);
    std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
    blob_storage->ExportBlobFiles(blob_files);
    ASSERT_EQ(count, blob_files.size());
  }

  ColumnFamilyHandle* GetColumnFamilyHandle(uint32_t cf_id) {
    return db_impl_->db_impl_->GetColumnFamilyHandleUnlocked(cf_id).release();
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

  void VerifyBlob(uint64_t file_number,
                  const std::map<std::string, std::string>& data) {
    // Open blob file and iterate in-file records
    EnvOptions env_opt;
    uint64_t file_size = 0;
    std::map<std::string, std::string> file_data;
    std::unique_ptr<RandomAccessFileReader> readable_file;
    std::string file_name = BlobFileName(options_.dirname, file_number);
    ASSERT_OK(env_->GetFileSize(file_name, &file_size));
    NewBlobFileReader(file_number, 0, options_, env_opt, env_, &readable_file);
    BlobFileIterator iter(std::move(readable_file), file_number, file_size,
                          options_);
    iter.SeekToFirst();
    for (auto& kv : data) {
      if (kv.second.size() < options_.min_blob_size) {
        continue;
      }
      ASSERT_EQ(iter.Valid(), true);
      ASSERT_EQ(iter.key(), kv.first);
      ASSERT_EQ(iter.value(), kv.second);
      iter.Next();
    }
  }

  // Note:
  // - When level is bottommost, always compact, no trivial move.
  void CompactAll(ColumnFamilyHandle* cf_handle = nullptr, int level = -1) {
    if (cf_handle == nullptr) {
      cf_handle = db_->DefaultColumnFamily();
    }
    auto opts = db_->GetOptions();
    if (level < 0) {
      level = opts.num_levels - 1;
    }
    auto compact_opts = CompactRangeOptions();
    compact_opts.change_level = true;
    compact_opts.target_level = level;
    if (level >= opts.num_levels - 1) {
      compact_opts.bottommost_level_compaction =
          BottommostLevelCompaction::kForce;
    }
    ASSERT_OK(db_->CompactRange(compact_opts, cf_handle, nullptr, nullptr));
  }

  void DeleteFilesInRange(const Slice* begin, const Slice* end) {
    RangePtr range(begin, end);
    ASSERT_OK(db_->DeleteFilesInRanges(db_->DefaultColumnFamily(), &range, 1));
    ASSERT_OK(
        db_->DeleteBlobFilesInRanges(db_->DefaultColumnFamily(), &range, 1));
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

  void TestTableFactory() {
    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
    Options options;
    options.create_if_missing = true;
    options.table_factory.reset(
        NewBlockBasedTableFactory(BlockBasedTableOptions()));
    auto* original_table_factory = options.table_factory.get();
    TitanDB* db;
    ASSERT_OK(TitanDB::Open(TitanOptions(options), dbname_, &db));
    auto cf_options = db->GetOptions(db->DefaultColumnFamily());
    auto db_options = db->GetDBOptions();
    ImmutableCFOptions immu_cf_options(cf_options);
    ASSERT_EQ(original_table_factory, immu_cf_options.table_factory.get());
    ASSERT_OK(db->Close());
    delete db;

    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
  }

  void SetBGError(const Status& s) {
    MutexLock l(&db_impl_->mutex_);
    db_impl_->SetBGError(s);
  }

  void CallGC() {
    {
      MutexLock l(&db_impl_->mutex_);
      db_impl_->bg_gc_scheduled_++;
    }
    db_impl_->BackgroundCallGC();
    {
      MutexLock l(&db_impl_->mutex_);
      while (db_impl_->bg_gc_scheduled_) {
        db_impl_->bg_cv_.Wait();
      }
    }
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

TEST_F(TitanDBTest, Open) {
  std::atomic<bool> checked_before_blob_file_set{false};
  std::atomic<bool> background_job_started{false};
  SyncPoint::GetInstance()->SetCallBack("TitanDBImpl::OnFlushCompleted:Begin",
                                        [&](void*) {
                                          background_job_started = true;
                                          assert(false);
                                        });
  SyncPoint::GetInstance()->SetCallBack(
      "TitanDBImpl::OnCompactionCompleted:Begin", [&](void*) {
        background_job_started = true;
        assert(false);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Open();
  ASSERT_FALSE(background_job_started.load());
}

TEST_F(TitanDBTest, AsyncInitializeGC) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"TitanDBTest::AsyncInitializeGC:ReleaseInitialization",
        "TitanDBImpl::AsyncInitializeGC:Begin"},
       {"TitanDBImpl::AsyncInitializeGC:End",
        "TitanDBTest::AsyncInitializeGC:Wait"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  options_.disable_background_gc = true;
  options_.blob_file_discardable_ratio = 0.01;
  options_.min_blob_size = 0;
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "bar", "v1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, GetBlobStorage().lock()->NumBlobFiles());
  ASSERT_OK(db_->Delete(WriteOptions(), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  uint32_t default_cf_id = db_->DefaultColumnFamily()->GetID();
  ASSERT_EQ(1, GetBlobStorage().lock()->NumBlobFiles());

  TEST_SYNC_POINT("TitanDBTest::AsyncInitializeGC:ReleaseInitialization");
  TEST_SYNC_POINT("TitanDBTest::AsyncInitializeGC:Wait");
  // GC the first blob file.
  ASSERT_OK(db_impl_->TEST_StartGC(default_cf_id));
  ASSERT_EQ(2, GetBlobStorage().lock()->NumBlobFiles());
  ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
  ASSERT_EQ(1, GetBlobStorage().lock()->NumBlobFiles());
  VerifyDB({{"bar", "v1"}});
}

TEST_F(TitanDBTest, Basic) {
  const uint64_t kNumKeys = 100;
  std::map<std::string, std::string> data;
  for (auto i = 0; i < 6; i++) {
    if (i == 0) {
      Open();
    } else {
      Reopen();
      VerifyDB(data);
      AddCF(std::to_string(i));
      if (i % 3 == 0) {
        DropCF(std::to_string(i - 1));
        DropCF(std::to_string(i - 2));
      }
    }
    for (uint64_t k = 1; k <= kNumKeys; k++) {
      Put(k, &data);
    }
    Flush();
    VerifyDB(data);
  }
}

TEST_F(TitanDBTest, DictCompressOptions) {
#if ZSTD_VERSION_NUMBER >= 10103
  options_.min_blob_size = 1;
  options_.blob_file_compression = CompressionType::kZSTD;
  options_.blob_file_compression_options.window_bits = -14;
  options_.blob_file_compression_options.level = 32767;
  options_.blob_file_compression_options.strategy = 0;
  options_.blob_file_compression_options.max_dict_bytes = 6400;
  options_.blob_file_compression_options.zstd_max_train_bytes = 0;

  const uint64_t kNumKeys = 500;
  std::map<std::string, std::string> data;
  Open();
  for (uint64_t k = 1; k <= kNumKeys; k++) {
    Put(k, &data);
  }
  Flush();
  VerifyDB(data);
#endif
}

TEST_F(TitanDBTest, TableFactory) { TestTableFactory(); }

TEST_F(TitanDBTest, DbIter) {
  Open();
  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  Flush();
  ASSERT_EQ(kNumEntries, data.size());
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  for (const auto& it : data) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(it.first, iter->key());
    ASSERT_EQ(it.second, iter->value());
    iter->Next();
  }
  ASSERT_FALSE(iter->Valid());
}

TEST_F(TitanDBTest, DBIterSeek) {
  Open();
  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  Flush();
  ASSERT_EQ(kNumEntries, data.size());
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(data.begin()->first, iter->key());
  ASSERT_EQ(data.begin()->second, iter->value());
  iter->SeekToLast();
  ASSERT_EQ(data.rbegin()->first, iter->key());
  ASSERT_EQ(data.rbegin()->second, iter->value());
  for (auto it = data.rbegin(); it != data.rend(); it++) {
    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    iter->SeekForPrev(it->first);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(it->first, iter->key());
    ASSERT_EQ(it->second, iter->value());
  }
  for (const auto& it : data) {
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    iter->Seek(it.first);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(it.first, iter->key());
    ASSERT_EQ(it.second, iter->value());
  }
}

TEST_F(TitanDBTest, GetProperty) {
  options_.disable_background_gc = false;
  Open();
  for (uint64_t k = 1; k <= 100; k++) {
    Put(k);
  }
  Flush();
  uint64_t value;
  ASSERT_TRUE(GetIntProperty(TitanDB::Properties::kNumLiveBlobFile, &value));
  ASSERT_EQ(value, 1);
  ASSERT_TRUE(
      GetIntProperty(TitanDB::Properties::kNumDiscardableRatioLE0File, &value));
  ASSERT_EQ(value, 1);

  Reopen();
  ASSERT_TRUE(GetIntProperty(TitanDB::Properties::kNumLiveBlobFile, &value));
  ASSERT_EQ(value, 1);
  ASSERT_TRUE(
      GetIntProperty(TitanDB::Properties::kNumDiscardableRatioLE0File, &value));
  ASSERT_EQ(value, 1);
  ASSERT_TRUE(GetIntProperty(TitanDB::Properties::kNumDiscardableRatioLE20File,
                             &value));
  ASSERT_EQ(value, 0);
  ASSERT_TRUE(GetIntProperty(TitanDB::Properties::kNumDiscardableRatioLE50File,
                             &value));
  ASSERT_EQ(value, 0);
  ASSERT_TRUE(GetIntProperty(TitanDB::Properties::kNumDiscardableRatioLE80File,
                             &value));
  ASSERT_EQ(value, 0);
  ASSERT_TRUE(GetIntProperty(TitanDB::Properties::kNumDiscardableRatioLE100File,
                             &value));
  ASSERT_EQ(value, 0);

  for (uint64_t k = 1; k <= 100; k++) {
    if (k % 3 == 0) Delete(k);
  }
  Flush();
  CompactAll();

  ASSERT_TRUE(
      GetIntProperty(TitanDB::Properties::kNumDiscardableRatioLE0File, &value));
  ASSERT_EQ(value, 0);
  ASSERT_TRUE(GetIntProperty(TitanDB::Properties::kNumDiscardableRatioLE50File,
                             &value));
  ASSERT_EQ(value, 1);

  Reopen();
  ASSERT_TRUE(
      GetIntProperty(TitanDB::Properties::kNumDiscardableRatioLE0File, &value));
  ASSERT_EQ(value, 0);
  ASSERT_TRUE(GetIntProperty(TitanDB::Properties::kNumDiscardableRatioLE50File,
                             &value));
  ASSERT_EQ(value, 1);

  for (uint64_t k = 1; k <= 100; k++) {
    if (k % 3 != 0) Delete(k);
  }
  Flush();
  CompactAll();

  db_impl_->TEST_WaitForBackgroundGC();
  ASSERT_TRUE(GetIntProperty(TitanDB::Properties::kNumLiveBlobFile, &value));
  ASSERT_EQ(value, 0);
  ASSERT_TRUE(
      GetIntProperty(TitanDB::Properties::kNumObsoleteBlobFile, &value));
  ASSERT_EQ(value, 1);
  ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
  ASSERT_TRUE(
      GetIntProperty(TitanDB::Properties::kNumObsoleteBlobFile, &value));
  ASSERT_EQ(value, 0);
  ASSERT_TRUE(GetIntProperty(TitanDB::Properties::kNumDiscardableRatioLE50File,
                             &value));
  ASSERT_EQ(value, 0);
}

TEST_F(TitanDBTest, Snapshot) {
  Open();
  std::map<std::string, std::string> data;
  Put(1, &data);
  ASSERT_EQ(1, data.size());

  const Snapshot* snapshot(db_->GetSnapshot());
  ReadOptions ropts;
  ropts.snapshot = snapshot;

  VerifyDB(data, ropts);
  Flush();
  VerifyDB(data, ropts);
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(TitanDBTest, IngestExternalFiles) {
  Open();
  SstFileWriter sst_file_writer(EnvOptions(), options_);
  ASSERT_EQ(sst_file_writer.FileSize(), 0);

  const uint64_t kNumEntries = 100;
  std::map<std::string, std::string> total_data;
  std::map<std::string, std::string> original_data;
  std::map<std::string, std::string> ingested_data;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &original_data);
  }
  ASSERT_EQ(kNumEntries, original_data.size());
  total_data.insert(original_data.begin(), original_data.end());
  VerifyDB(total_data);
  Flush();
  VerifyDB(total_data);

  const uint64_t kNumIngestedEntries = 100;
  // Make sure that keys in SST overlaps with existing keys
  const uint64_t kIngestedStart = kNumEntries - kNumEntries / 2;
  std::string sst_file = options_.dirname + "/for_ingest.sst";
  ASSERT_OK(sst_file_writer.Open(sst_file));
  for (uint64_t i = 1; i <= kNumIngestedEntries; i++) {
    std::string key = GenKey(kIngestedStart + i);
    std::string value = GenValue(kIngestedStart + i);
    ASSERT_OK(sst_file_writer.Put(key, value));
    total_data[key] = value;
    ingested_data.emplace(key, value);
  }
  ASSERT_OK(sst_file_writer.Finish());
  IngestExternalFileOptions ifo;
  ASSERT_OK(db_->IngestExternalFile({sst_file}, ifo));
  VerifyDB(total_data);
  Flush();
  VerifyDB(total_data);
  for (auto& handle : cf_handles_) {
    auto blob = GetBlobStorage(handle);
    ASSERT_EQ(1, blob.lock()->NumBlobFiles());
  }

  CompactRangeOptions copt;
  ASSERT_OK(db_->CompactRange(copt, nullptr, nullptr));
  VerifyDB(total_data);
  for (auto& handle : cf_handles_) {
    auto blob = GetBlobStorage(handle);
    ASSERT_EQ(2, blob.lock()->NumBlobFiles());
    std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
    blob.lock()->ExportBlobFiles(blob_files);
    ASSERT_EQ(2, blob_files.size());
    auto bf = blob_files.begin();
    VerifyBlob(bf->first, original_data);
    bf++;
    VerifyBlob(bf->first, ingested_data);
  }
}

TEST_F(TitanDBTest, NewColumnFamilyHasBlobFileSizeCollector) {
  Open();
  AddCF("new_cf");
  Options opt = db_->GetOptions(cf_handles_.back());
  ASSERT_EQ(1, opt.table_properties_collector_factories.size());
  std::unique_ptr<BlobFileSizeCollectorFactory> prop_collector_factory(
      new BlobFileSizeCollectorFactory());
  ASSERT_EQ(std::string(prop_collector_factory->Name()),
            std::string(opt.table_properties_collector_factories[0]->Name()));
}

TEST_F(TitanDBTest, DropColumnFamily) {
  Open();
  const uint64_t kNumCF = 3;
  for (uint64_t i = 1; i <= kNumCF; i++) {
    AddCF(std::to_string(i));
  }
  const uint64_t kNumEntries = 100;
  std::map<std::string, std::string> data;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  VerifyDB(data);
  Flush();
  VerifyDB(data);

  // Destroy column families handle, check whether the data is preserved after a
  // round of GC and restart.
  for (auto& handle : cf_handles_) {
    db_->DestroyColumnFamilyHandle(handle);
  }
  cf_handles_.clear();
  VerifyDB(data);
  Reopen();
  VerifyDB(data);

  for (auto& handle : cf_handles_) {
    // we can't drop default column family
    if (handle->GetName() == kDefaultColumnFamilyName) {
      continue;
    }
    ASSERT_OK(db_->DropColumnFamily(handle));
    // The data is actually deleted only after destroying all outstanding column
    // family handles, so we can still read from the dropped column family.
    VerifyDB(data);
  }

  Close();
}

TEST_F(TitanDBTest, DestroyColumnFamilyHandle) {
  Open();
  const uint64_t kNumCF = 3;
  for (uint64_t i = 1; i <= kNumCF; i++) {
    AddCF(std::to_string(i));
  }
  const uint64_t kNumEntries = 10;
  std::map<std::string, std::string> data;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  VerifyDB(data);
  Flush();
  VerifyDB(data);

  // Destroy column families handle, check whether GC skips the column families.
  for (auto& handle : cf_handles_) {
    auto cf_id = handle->GetID();
    db_->DestroyColumnFamilyHandle(handle);
    ASSERT_OK(db_impl_->TEST_StartGC(cf_id));
  }
  cf_handles_.clear();
  VerifyDB(data);

  Reopen();
  VerifyDB(data);
  Close();
}

TEST_F(TitanDBTest, DeleteFilesInRange) {
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), GenKey(11), GenValue(1)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(21), GenValue(2)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(31), GenValue(3)));
  Flush();
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(41), GenValue(4)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(51), GenValue(5)));
  Flush();
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(61), GenValue(6)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(71), GenValue(7)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(81), GenValue(8)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(91), GenValue(9)));
  Flush();
  // Not bottommost, we need trivial move.
  CompactAll(nullptr, 5);

  std::string value;
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &value));
  ASSERT_EQ(value, "0");
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level5", &value));
  ASSERT_EQ(value, "3");

  ASSERT_OK(db_->Put(WriteOptions(), GenKey(12), GenValue(1)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(22), GenValue(2)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(32), GenValue(3)));
  Flush();
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(42), GenValue(4)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(52), GenValue(5)));
  Flush();
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(62), GenValue(6)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(72), GenValue(7)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(82), GenValue(8)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(92), GenValue(9)));
  Flush();

  // The LSM structure is:
  // L0: [11, 21, 31] [41, 51] [61, 71, 81, 91]
  // L5: [12, 22, 32] [42, 52] [62, 72, 82, 92]
  // with 6 alive blob files
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &value));
  ASSERT_EQ(value, "3");
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level5", &value));
  ASSERT_EQ(value, "3");

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());

  std::string key40 = GenKey(40);
  std::string key80 = GenKey(80);
  Slice start = Slice(key40);
  Slice end = Slice(key80);
  DeleteFilesInRange(&start, &end);

  // Now the LSM structure is:
  // L0: [11, 21, 31] [41, 51] [61, 71, 81, 91]
  // L5: [12, 22, 32]          [62, 72, 82, 92]
  // with 4 alive blob files and 2 obsolete blob files
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &value));
  ASSERT_EQ(value, "3");
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level5", &value));
  ASSERT_EQ(value, "2");

  auto blob = GetBlobStorage(db_->DefaultColumnFamily()).lock();
  ASSERT_EQ(blob->NumBlobFiles(), 6);
  // These two files are marked obsolete directly by `DeleteBlobFilesInRanges`
  ASSERT_EQ(blob->NumObsoleteBlobFiles(), 2);

  // The snapshot held by the iterator prevents the blob files from being
  // purged.
  ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
  while (iter->Valid()) {
    iter->Next();
    ASSERT_OK(iter->status());
  }
  ASSERT_EQ(blob->NumBlobFiles(), 6);
  ASSERT_EQ(blob->NumObsoleteBlobFiles(), 2);

  // Once the snapshot is released, the blob files should be purged.
  iter.reset(nullptr);
  ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
  ASSERT_EQ(blob->NumBlobFiles(), 4);
  ASSERT_EQ(blob->NumObsoleteBlobFiles(), 0);

  Close();
}

TEST_F(TitanDBTest, VersionEditError) {
  Open();

  std::map<std::string, std::string> data;
  Put(1, &data);
  ASSERT_EQ(1, data.size());
  VerifyDB(data);

  auto cf_id = db_->DefaultColumnFamily()->GetID();
  VersionEdit edit;
  edit.SetColumnFamilyID(cf_id);
  edit.AddBlobFile(std::make_shared<BlobFileMeta>(1, 1, 0, 0, "", ""));
  ASSERT_OK(LogAndApply(edit));

  VerifyDB(data);

  // add same blob file twice
  VersionEdit edit1;
  edit1.SetColumnFamilyID(cf_id);
  edit1.AddBlobFile(std::make_shared<BlobFileMeta>(1, 1, 0, 0, "", ""));
  ASSERT_NOK(LogAndApply(edit));

  Reopen();
  VerifyDB(data);
}

#ifndef NDEBUG
TEST_F(TitanDBTest, BlobFileIOError) {
  std::unique_ptr<TitanFaultInjectionTestEnv> mock_env(
      new TitanFaultInjectionTestEnv(env_));
  options_.env = mock_env.get();
  options_.disable_background_gc = true;  // avoid abort by BackgroundGC
  Open();

  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  ASSERT_EQ(kNumEntries, data.size());
  CompactRangeOptions copts;
  ASSERT_OK(db_->CompactRange(copts, nullptr, nullptr));
  VerifyDB(data);

  SyncPoint::GetInstance()->SetCallBack("BlobFileReader::Get", [&](void*) {
    mock_env->SetFilesystemActive(false, Status::IOError("Injected error"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  for (auto& it : data) {
    std::string value;
    if (it.second.size() > options_.min_blob_size) {
      ASSERT_TRUE(db_->Get(ReadOptions(), it.first, &value).IsIOError());
      mock_env->SetFilesystemActive(true);
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
  mock_env->SetFilesystemActive(true);

  TitanReadOptions opts;
  opts.abort_on_failure = false;
  std::unique_ptr<Iterator> iter(db_->NewIterator(opts));
  SyncPoint::GetInstance()->EnableProcessing();
  iter->SeekToFirst();
  iter->value();
  ASSERT_TRUE(iter->status().IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  mock_env->SetFilesystemActive(true);

  iter.reset(db_->NewIterator(opts));
  iter->SeekToFirst();
  iter->value();
  ASSERT_TRUE(iter->Valid());
  SyncPoint::GetInstance()->EnableProcessing();
  iter->Next();  // second value (k=2) is inlined
  iter->value();
  ASSERT_TRUE(iter->Valid());
  iter->Next();
  iter->value();
  ASSERT_TRUE(iter->status().IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  mock_env->SetFilesystemActive(true);

  options_.env = env_;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // env must be destructed AFTER db is closed to avoid
  // `pure abstract method called` complaint.
  iter.reset(nullptr);  // early release to avoid outstanding reference
  Close();
  db_ = nullptr;
}

TEST_F(TitanDBTest, FlushWriteIOErrorHandling) {
  std::unique_ptr<TitanFaultInjectionTestEnv> mock_env(
      new TitanFaultInjectionTestEnv(env_));
  options_.env = mock_env.get();
  options_.disable_background_gc = true;  // avoid abort by BackgroundGC
  Open();

  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  ASSERT_EQ(kNumEntries, data.size());
  CompactRangeOptions copts;
  // no compaction to enable Flush
  VerifyDB(data);

  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    mock_env->SetFilesystemActive(false,
                                  Status::IOError("FlushJob injected error"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  FlushOptions fopts;
  ASSERT_TRUE(db_->Flush(fopts).IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  mock_env->SetFilesystemActive(true);
  // subsequent writes return error too
  WriteOptions wopts;
  std::string key = "key_after_flush";
  std::string value = "value_after_flush";
  ASSERT_TRUE(db_->Put(wopts, key, value).IsIOError());

  options_.env = env_;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // env must be destructed AFTER db is closed to avoid
  // `pure abstract method called` complaint.
  Close();
  db_ = nullptr;
}

TEST_F(TitanDBTest, CompactionWriteIOErrorHandling) {
  std::unique_ptr<TitanFaultInjectionTestEnv> mock_env(
      new TitanFaultInjectionTestEnv(env_));
  options_.env = mock_env.get();
  options_.disable_background_gc = true;  // avoid abort by BackgroundGC
  Open();

  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  ASSERT_EQ(kNumEntries, data.size());
  CompactRangeOptions copts;
  // do not compact to enable following Compaction
  VerifyDB(data);

  SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        mock_env->SetFilesystemActive(
            false, Status::IOError("Compaction injected error"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_TRUE(db_->CompactRange(copts, nullptr, nullptr).IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  mock_env->SetFilesystemActive(true);
  // subsequent writes return error too
  WriteOptions wopts;
  std::string key = "key_after_compaction";
  std::string value = "value_after_compaction";
  ASSERT_TRUE(db_->Put(wopts, key, value).IsIOError());

  options_.env = env_;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // env must be destructed AFTER db is closed to avoid
  // `pure abstract method called` complaint.
  Close();
  db_ = nullptr;
}

TEST_F(TitanDBTest, BlobFileCorruptionErrorHandling) {
  options_.disable_background_gc = true;  // avoid abort by BackgroundGC
  Open();
  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  ASSERT_EQ(kNumEntries, data.size());
  CompactRangeOptions copt;
  ASSERT_OK(db_->CompactRange(copt, nullptr, nullptr));
  VerifyDB(data);

  // Modify the checksum data to reproduce a mismatch
  SyncPoint::GetInstance()->SetCallBack(
      "BlobDecoder::DecodeRecord", [&](void* arg) {
        auto* crc = reinterpret_cast<uint32_t*>(arg);
        *crc = *crc + 1;
      });

  SyncPoint::GetInstance()->EnableProcessing();
  for (auto& it : data) {
    std::string value;
    if (it.second.size() < options_.min_blob_size) {
      continue;
    }
    ASSERT_TRUE(db_->Get(ReadOptions(), it.first, &value).IsCorruption());
  }
  SyncPoint::GetInstance()->DisableProcessing();

  TitanReadOptions opts;
  opts.abort_on_failure = false;
  std::unique_ptr<Iterator> iter(db_->NewIterator(opts));
  SyncPoint::GetInstance()->EnableProcessing();
  iter->SeekToFirst();
  iter->value();
  ASSERT_TRUE(iter->status().IsCorruption());
  SyncPoint::GetInstance()->DisableProcessing();

  iter.reset(db_->NewIterator(opts));
  iter->SeekToFirst();
  iter->value();
  ASSERT_TRUE(iter->Valid());
  SyncPoint::GetInstance()->EnableProcessing();
  iter->Next();  // second value (k=2) is inlined
  iter->value();
  ASSERT_TRUE(iter->Valid());
  iter->Next();
  iter->value();
  ASSERT_TRUE(iter->status().IsCorruption());
  SyncPoint::GetInstance()->DisableProcessing();

  SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // !NDEBUG

TEST_F(TitanDBTest, SetOptions) {
  options_.write_buffer_size = 42000000;
  options_.min_blob_size = 123;
  options_.blob_run_mode = TitanBlobRunMode::kReadOnly;
  Open();

  TitanOptions titan_options = db_->GetTitanOptions();
  ASSERT_EQ(42000000, titan_options.write_buffer_size);
  ASSERT_EQ(123, titan_options.min_blob_size);
  ASSERT_EQ(TitanBlobRunMode::kReadOnly, titan_options.blob_run_mode);

  std::unordered_map<std::string, std::string> opts;

  // Set titan options.
  opts["blob_run_mode"] = "kReadOnly";
  ASSERT_OK(db_->SetOptions(opts));
  titan_options = db_->GetTitanOptions();
  ASSERT_EQ(TitanBlobRunMode::kReadOnly, titan_options.blob_run_mode);
  opts.clear();
  opts["min_blob_size"] = "456";
  ASSERT_OK(db_->SetOptions(opts));
  titan_options = db_->GetTitanOptions();
  ASSERT_EQ(456, titan_options.min_blob_size);
  opts.clear();

  // Set column family options.
  opts["disable_auto_compactions"] = "true";
  ASSERT_OK(db_->SetOptions(opts));
  titan_options = db_->GetTitanOptions();
  ASSERT_TRUE(titan_options.disable_auto_compactions);
  opts.clear();

  // Set DB options.
  opts["max_background_jobs"] = "15";
  ASSERT_OK(db_->SetDBOptions(opts));
  titan_options = db_->GetTitanOptions();
  ASSERT_EQ(15, titan_options.max_background_jobs);
  TitanDBOptions titan_db_options = db_->GetTitanDBOptions();
  ASSERT_EQ(15, titan_db_options.max_background_jobs);
}

TEST_F(TitanDBTest, BlobRunModeBasic) {
  options_.disable_background_gc = true;
  options_.merge_small_file_threshold = 0;
  options_.disable_auto_compactions = true;
  Open();

  const uint64_t kNumEntries = 100;
  const uint64_t kMaxKeys = 100000;
  uint64_t begin;
  std::unordered_map<std::string, std::string> opts;
  std::map<std::string, std::string> data;
  std::vector<KeyVersion> version;
  std::string begin_key;
  std::string end_key;
  uint64_t num_blob_files;

  begin = 1;
  for (uint64_t i = begin; i < begin + kNumEntries; i++) {
    Put(i, &data);
  }
  begin_key = GenKey(begin);
  end_key = GenKey(begin + kNumEntries - 1);
  ASSERT_EQ(kNumEntries, data.size());
  VerifyDB(data);
  Flush();
  auto blob = GetBlobStorage();
  num_blob_files = blob.lock()->NumBlobFiles();
  VerifyDB(data);
  GetAllKeyVersions(db_, begin_key, end_key, kMaxKeys, &version);
  for (auto v : version) {
    if (data[v.user_key].size() >= options_.min_blob_size) {
      ASSERT_EQ(v.type, static_cast<int>(ValueType::kTypeBlobIndex));
    } else {
      ASSERT_EQ(v.type, static_cast<int>(ValueType::kTypeValue));
    }
  }
  version.clear();

  opts["blob_run_mode"] = "kReadOnly";
  db_->SetOptions(opts);
  begin = kNumEntries + 1;
  for (uint64_t i = begin; i < begin + kNumEntries; i++) {
    Put(i, &data);
  }
  begin_key = GenKey(begin);
  end_key = GenKey(begin + kNumEntries - 1);
  ASSERT_EQ(kNumEntries * 2, data.size());
  VerifyDB(data);
  Flush();
  blob = GetBlobStorage();
  ASSERT_EQ(num_blob_files, blob.lock()->NumBlobFiles());
  VerifyDB(data);
  GetAllKeyVersions(db_, begin_key, end_key, kMaxKeys, &version);
  for (auto v : version) {
    ASSERT_EQ(v.type, static_cast<int>(ValueType::kTypeValue));
  }
  version.clear();

  opts["blob_run_mode"] = "kFallback";
  db_->SetOptions(opts);
  begin = kNumEntries * 2 + 1;
  for (uint64_t i = begin; i < begin + kNumEntries; i++) {
    Put(i, &data);
  }
  begin_key = GenKey(begin);
  end_key = GenKey(begin + kNumEntries - 1);
  ASSERT_EQ(kNumEntries * 3, data.size());
  VerifyDB(data);
  Flush();
  blob = GetBlobStorage();
  ASSERT_EQ(num_blob_files, blob.lock()->NumBlobFiles());
  VerifyDB(data);
  GetAllKeyVersions(db_, begin_key, end_key, kMaxKeys, &version);
  for (auto v : version) {
    ASSERT_EQ(v.type, static_cast<int>(ValueType::kTypeValue));
  }
  version.clear();

  // make sure new sstable interleaves with existing sstables.
  Put(0, &data);
  Put(kNumEntries * 3 + 1, &data);
  Flush();
  CompactAll();
  VerifyDB(data);
  uint32_t default_cf_id = db_->DefaultColumnFamily()->GetID();
  ASSERT_OK(db_impl_->TEST_StartGC(default_cf_id));
  ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
  blob = GetBlobStorage();
  ASSERT_EQ(0, blob.lock()->NumBlobFiles());
  begin_key = GenKey(0);
  end_key = GenKey(kNumEntries * 3 + 1);
  GetAllKeyVersions(db_, begin_key, end_key, kMaxKeys, &version);
  for (auto v : version) {
    ASSERT_EQ(v.type, static_cast<int>(ValueType::kTypeValue));
  }
  version.clear();
}

TEST_F(TitanDBTest, FallbackModeEncounterMissingBlobFile) {
  options_.disable_background_gc = true;
  options_.blob_file_discardable_ratio = 0.01;
  options_.min_blob_size = true;
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "bar", "v1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, GetBlobStorage().lock()->NumBlobFiles());
  ASSERT_OK(db_->Delete(WriteOptions(), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  uint32_t default_cf_id = db_->DefaultColumnFamily()->GetID();
  // GC the first blob file.
  ASSERT_OK(db_impl_->TEST_StartGC(default_cf_id));
  ASSERT_EQ(2, GetBlobStorage().lock()->NumBlobFiles());
  ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
  ASSERT_EQ(1, GetBlobStorage().lock()->NumBlobFiles());
  ASSERT_OK(db_->SetOptions({{"blob_run_mode", "kFallback"}}));
  // Run compaction in fallback mode. Make sure it correctly handle the
  // missing blob file.
  Slice begin("foo");
  Slice end("foo1");
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &begin, &end));
  VerifyDB({{"bar", "v1"}});
}

TEST_F(TitanDBTest, GCInFallbackMode) {
  options_.disable_background_gc = true;
  options_.min_blob_size = 0;
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "bar", "v1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, GetBlobStorage().lock()->NumBlobFiles());
  ASSERT_OK(db_->Delete(WriteOptions(), "foo"));
  ASSERT_OK(db_->Delete(WriteOptions(), "bar"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  uint32_t default_cf_id = db_->DefaultColumnFamily()->GetID();
  ASSERT_EQ(1, GetBlobStorage().lock()->NumBlobFiles());
  ASSERT_OK(db_->SetOptions({{"blob_run_mode", "kFallback"}}));
  // GC the first blob file.
  ASSERT_OK(db_impl_->TEST_StartGC(default_cf_id));
  ASSERT_EQ(1, GetBlobStorage().lock()->NumBlobFiles());
  ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
  ASSERT_EQ(0, GetBlobStorage().lock()->NumBlobFiles());
  VerifyDB({});
}

TEST_F(TitanDBTest, BackgroundErrorHandling) {
  options_.listeners.emplace_back(std::make_shared<BGErrorListener>());
  Open();
  std::string key = "key", val = "val";
  SetBGError(Status::IOError(""));
  // BG error is restored by listener for first time
  ASSERT_OK(db_->Put(WriteOptions(), key, val));
  SetBGError(Status::IOError(""));
  ASSERT_OK(db_->Get(ReadOptions(), key, &val));
  ASSERT_EQ(val, "val");
  ASSERT_TRUE(db_->Put(WriteOptions(), key, val).IsIOError());
  ASSERT_TRUE(db_->Flush(FlushOptions()).IsIOError());
  ASSERT_TRUE(db_->Delete(WriteOptions(), key).IsIOError());
  ASSERT_TRUE(
      db_->CompactRange(CompactRangeOptions(), nullptr, nullptr).IsIOError());
  ASSERT_TRUE(
      db_->CompactFiles(CompactionOptions(), std::vector<std::string>(), 1)
          .IsIOError());
  Close();
}
TEST_F(TitanDBTest, BackgroundErrorTrigger) {
  std::unique_ptr<TitanFaultInjectionTestEnv> mock_env(
      new TitanFaultInjectionTestEnv(env_));
  options_.env = mock_env.get();
  Open();
  std::map<std::string, std::string> data;
  const int kNumEntries = 100;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Put(i, &data);
  }
  Flush();
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    Delete(i);
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileSet::LogAndApply::Begin", [&](void*) {
        mock_env->SetFilesystemActive(false, Status::IOError("Injected error"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  CallGC();
  mock_env->SetFilesystemActive(true);
  // Still failed for bg error
  ASSERT_TRUE(db_impl_->Put(WriteOptions(), "key", "val").IsIOError());
  Close();
}

// Make sure DropColumnFamilies() will wait if there's running GC job.
TEST_F(TitanDBTest, DropCFWhileGC) {
  options_.min_blob_size = 0;
  options_.blob_file_discardable_ratio = 0.1;
  options_.disable_background_gc = false;
  Open();

  // Create CF.
  std::vector<TitanCFDescriptor> descs = {{"new_cf", options_}};
  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_OK(db_->CreateColumnFamilies(descs, &handles));
  ASSERT_EQ(1, handles.size());
  auto* cfh = handles[0];

  SyncPoint::GetInstance()->LoadDependency(
      {{"TitanDBImpl::BackgroundCallGC:BeforeBackgroundGC",
        "TitanDBImpl::DropColumnFamilies:Begin"}});
  SyncPoint::GetInstance()->SetCallBack(
      "TitanDBImpl::DropColumnFamilies:BeforeBaseDBDropCF",
      [&](void*) { ASSERT_EQ(0, db_impl_->TEST_bg_gc_running()); });
  SyncPoint::GetInstance()->EnableProcessing();

  // Create two blob files, and trigger GC of the first one.
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "bar", "v1"));
  ASSERT_OK(db_->Flush(FlushOptions(), cfh));
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "foo", "v2"));
  ASSERT_OK(db_->Flush(FlushOptions(), cfh));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), cfh, nullptr, nullptr));

  // Verify no GC job is running while we drop the CF.
  ASSERT_OK(db_->DropColumnFamilies(handles));

  // Cleanup.
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cfh));
  Close();
}

// Make sure GC job will not run on a dropped CF.
TEST_F(TitanDBTest, GCAfterDropCF) {
  options_.min_blob_size = 0;
  options_.blob_file_discardable_ratio = 0.1;
  options_.disable_background_gc = false;
  Open();

  // Create CF.
  std::vector<TitanCFDescriptor> descs = {{"new_cf", options_}};
  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_OK(db_->CreateColumnFamilies(descs, &handles));
  ASSERT_EQ(1, handles.size());
  auto* cfh = handles[0];

  std::atomic<int> skip_dropped_cf_count{0};

  SyncPoint::GetInstance()->LoadDependency(
      {{"TitanDBTest::GCAfterDropCF:AfterDropCF",
        "TitanDBImpl::BackgroundCallGC:BeforeGCRunning"},
       {"TitanDBImpl::BackgroundGC:Finish",
        "TitanDBTest::GCAfterDropCF:WaitGC"}});
  SyncPoint::GetInstance()->SetCallBack(
      "TitanDBImpl::BackgroundGC:CFDropped",
      [&](void*) { skip_dropped_cf_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Create two blob files, and trigger GC of the first one.
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "bar", "v1"));
  ASSERT_OK(db_->Flush(FlushOptions(), cfh));
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "foo", "v2"));
  ASSERT_OK(db_->Flush(FlushOptions(), cfh));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), cfh, nullptr, nullptr));

  // Drop CF before GC runs. Check if GC job skip the dropped CF.
  ASSERT_OK(db_->DropColumnFamilies(handles));
  TEST_SYNC_POINT("TitanDBTest::GCAfterDropCF:AfterDropCF");
  TEST_SYNC_POINT("TitanDBTest::GCAfterDropCF:WaitGC");
  ASSERT_EQ(1, skip_dropped_cf_count.load());

  // Cleanup.
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cfh));
  Close();
}

TEST_F(TitanDBTest, GCBeforeFlushCommit) {
  port::Mutex mu;
  port::CondVar cv(&mu);
  std::atomic<bool> is_first_flush{true};
  std::atomic<int> flush_completed{0};
  DBImpl* db_impl = nullptr;

  SyncPoint::GetInstance()->LoadDependency(
      {{"TitanDBTest::GCBeforeFlushCommit:PauseInstall",
        "TitanDBTest::GCBeforeFlushCommit:WaitFlushPause"}});
  SyncPoint::GetInstance()->SetCallBack("FlushJob::InstallResults", [&](void*) {
    if (is_first_flush) {
      is_first_flush = false;
    } else {
      // skip waiting for the second flush.
      return;
    }
    auto* db_mutex = db_impl->mutex();
    db_mutex->Unlock();
    TEST_SYNC_POINT("TitanDBTest::GCBeforeFlushCommit:PauseInstall");
    Env::Default()->SleepForMicroseconds(1000 * 1000);  // 1s
    db_mutex->Lock();
  });
  SyncPoint::GetInstance()->SetCallBack(
      "TitanDBImpl::OnFlushCompleted:Finished", [&](void*) {
        MutexLock l(&mu);
        flush_completed++;
        cv.SignalAll();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "TitanDBTest::GCBeforeFlushCommit:WaitSecondFlush", [&](void*) {
        MutexLock l(&mu);
        while (flush_completed < 2) {
          cv.Wait();
        }
      });

  options_.create_if_missing = true;
  // Setting max_flush_jobs = max_background_jobs / 4 = 2.
  options_.max_background_jobs = 8;
  options_.max_write_buffer_number = 4;
  options_.min_blob_size = 0;
  options_.merge_small_file_threshold = 1024 * 1024;
  options_.disable_background_gc = true;
  Open();
  uint32_t cf_id = db_->DefaultColumnFamily()->GetID();

  db_impl = reinterpret_cast<DBImpl*>(db_->GetRootDB());
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v"));
  // t1 will wait for the second flush complete before install super version.
  auto t1 = port::Thread([&]() {
    // flush_opts.wait = true
    ASSERT_OK(db_->Flush(FlushOptions()));
  });
  TEST_SYNC_POINT("TitanDBTest::GCBeforeFlushCommit:WaitFlushPause");
  // In the second flush we check if memtable has been committed, and signal
  // the first flush to proceed.
  ASSERT_OK(db_->Put(WriteOptions(), "bar", "v"));
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(db_->Flush(flush_opts));
  TEST_SYNC_POINT("TitanDBTest::GCBeforeFlushCommit:WaitSecondFlush");
  // Set live data size to force GC select the file.
  auto blob_storage = GetBlobStorage().lock();
  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
  blob_storage->ExportBlobFiles(blob_files);
  ASSERT_EQ(2, blob_files.size());
  // Set live data size to 0 to force GC.
  auto second_file = blob_files.rbegin()->second.lock();
  second_file->set_live_data_size(0);
  ASSERT_OK(db_impl_->TEST_StartGC(cf_id));
  ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
  t1.join();
  // Check value after memtable committed.
  std::string value;
  // Before fixing the issue, this call will return
  // Corruption: Missing blob file error.
  ASSERT_OK(db_->Get(ReadOptions(), "bar", &value));
  ASSERT_EQ("v", value);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Test GC stats will be recover on DB reopen and GC resume after reopen.
TEST_F(TitanDBTest, GCAfterReopen) {
  options_.min_blob_size = 0;
  options_.blob_file_discardable_ratio = 0.01;
  options_.disable_background_gc = true;
  options_.blob_file_compression = CompressionType::kNoCompression;

  // Generate a blob file and delete half of keys in it.
  Open();
  for (int i = 0; i < 100; i++) {
    std::string key = GenKey(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "v"));
  }
  Flush();

  std::shared_ptr<BlobStorage> blob_storage = GetBlobStorage().lock();
  ASSERT_TRUE(blob_storage != nullptr);
  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
  blob_storage->ExportBlobFiles(blob_files);
  ASSERT_EQ(1, blob_files.size());
  std::shared_ptr<BlobFileMeta> file1 = blob_files.begin()->second.lock();
  ASSERT_TRUE(file1 != nullptr);
  ASSERT_EQ(file1->GetDiscardableRatioLevel(),
            TitanInternalStats::NUM_DISCARDABLE_RATIO_LE0);

  for (int i = 0; i < 100; i++) {
    if (i % 2 == 0) {
      Delete(i);
    }
  }
  Flush();
  CompactAll();
  blob_storage->ExportBlobFiles(blob_files);
  ASSERT_EQ(1, blob_files.size());
  ASSERT_TRUE(abs(file1->GetDiscardableRatio() - 0.5) < 0.01);
  uint64_t file_number1 = file1->file_number();
  file1.reset();
  blob_files.clear();
  blob_storage.reset();

  // Sync point to verify GC stat recovered after reopen.
  std::atomic<int> num_gc_job{0};
  SyncPoint::GetInstance()->SetCallBack(
      "TitanDBImpl::AsyncInitializeGC:BeforeSetInitialized", [&](void* arg) {
        TitanDBImpl* db_impl = reinterpret_cast<TitanDBImpl*>(arg);
        blob_storage =
            db_impl->TEST_GetBlobStorage(db_impl->DefaultColumnFamily());
        ASSERT_TRUE(blob_storage != nullptr);
        blob_storage->ExportBlobFiles(blob_files);
        ASSERT_EQ(1, blob_files.size());
        std::shared_ptr<BlobFileMeta> file = blob_files.begin()->second.lock();
        ASSERT_TRUE(file != nullptr);
        ASSERT_TRUE(abs(file->GetDiscardableRatio() - 0.5) < 0.01);
      });
  SyncPoint::GetInstance()->SetCallBack("TitanDBImpl::BackgroundGC:Finish",
                                        [&](void*) { num_gc_job++; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Re-enable background GC and Reopen. See if GC resume.
  options_.disable_background_gc = false;
  Reopen();
  db_impl_->TEST_WaitForBackgroundGC();
  ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
  ASSERT_EQ(1, num_gc_job);
  blob_storage = GetBlobStorage().lock();
  ASSERT_TRUE(blob_storage != nullptr);
  blob_storage->ExportBlobFiles(blob_files);
  ASSERT_EQ(1, blob_files.size());
  std::shared_ptr<BlobFileMeta> file2 = blob_files.begin()->second.lock();
  ASSERT_GT(file2->file_number(), file_number1);
}

TEST_F(TitanDBTest, VeryLargeValue) {
  Open();

  ASSERT_OK(
      db_->Put(WriteOptions(), "k1", std::string(100 * 1024 * 1024, 'v')));
  Flush();

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value.size(), 100 * 1024 * 1024);

  Close();
}

TEST_F(TitanDBTest, UpdateValue) {
  options_.min_blob_size = 1024;
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10, 'v')));
  Flush();
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value.size(), 10);
  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100, 'v')));
  Flush();
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value.size(), 100);
  CompactAll();
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value.size(), 100);

  Close();
}

TEST_F(TitanDBTest, MultiGet) {
  options_.min_blob_size = 1024;
  std::vector<int> blob_cache_sizes = {0, 15 * 1024};
  std::vector<int> block_cache_sizes = {0, 150};

  for (auto blob_cache_size : blob_cache_sizes) {
    for (auto block_cache_size : block_cache_sizes) {
      BlockBasedTableOptions table_opts;
      table_opts.block_size = block_cache_size;
      options_.table_factory.reset(NewBlockBasedTableFactory(table_opts));
      options_.blob_cache = NewLRUCache(blob_cache_size);

      Open();
      ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100, 'v')));
      ASSERT_OK(db_->Put(WriteOptions(), "k2", std::string(100, 'v')));
      ASSERT_OK(db_->Put(WriteOptions(), "k3", std::string(10 * 1024, 'v')));
      ASSERT_OK(db_->Put(WriteOptions(), "k4", std::string(100 * 1024, 'v')));
      Flush();

      std::vector<std::string> values;
      db_->MultiGet(ReadOptions(), std::vector<Slice>{"k1", "k2", "k3", "k4"},
                    &values);
      ASSERT_EQ(values[0].size(), 100);
      ASSERT_EQ(values[1].size(), 100);
      ASSERT_EQ(values[2].size(), 10 * 1024);
      ASSERT_EQ(values[3].size(), 100 * 1024);
      Close();
      DeleteDir(env_, options_.dirname);
      DeleteDir(env_, dbname_);
    }
  }
}

TEST_F(TitanDBTest, PrefixScan) {
  options_.min_blob_size = 1024;
  options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
  std::vector<int> blob_cache_sizes = {0, 15 * 1024};
  std::vector<int> block_cache_sizes = {0, 150};

  for (auto blob_cache_size : blob_cache_sizes) {
    for (auto block_cache_size : block_cache_sizes) {
      BlockBasedTableOptions table_opts;
      table_opts.block_size = block_cache_size;
      options_.table_factory.reset(NewBlockBasedTableFactory(table_opts));
      options_.blob_cache = NewLRUCache(blob_cache_size);

      Open();
      ASSERT_OK(db_->Put(WriteOptions(), "abc1", std::string(100, 'v')));
      ASSERT_OK(db_->Put(WriteOptions(), "abc2", std::string(2 * 1024, 'v')));
      ASSERT_OK(db_->Put(WriteOptions(), "cba1", std::string(100, 'v')));
      ASSERT_OK(db_->Put(WriteOptions(), "cba1", std::string(10 * 1024, 'v')));
      Flush();

      ReadOptions r_opt;
      r_opt.prefix_same_as_start = true;
      {
        std::unique_ptr<Iterator> iter(db_->NewIterator(r_opt));
        iter->Seek("abc");
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("abc1", iter->key());
        ASSERT_EQ(iter->value().size(), 100);
        iter->Next();

        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("abc2", iter->key());
        ASSERT_EQ(iter->value().size(), 2 * 1024);
        iter->Next();

        ASSERT_FALSE(iter->Valid());
        ;
      }
      Close();
      DeleteDir(env_, options_.dirname);
      DeleteDir(env_, dbname_);
    }
  }
}

TEST_F(TitanDBTest, CompressionTypes) {
  options_.min_blob_size = 1024;
  auto compressions = std::vector<CompressionType>{
      CompressionType::kNoCompression, CompressionType::kLZ4Compression,
      CompressionType::kSnappyCompression, CompressionType::kZSTD};

  for (auto type : compressions) {
    options_.blob_file_compression = type;
    Open();
    ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
    Flush();
    Close();
  }
}

TEST_F(TitanDBTest, DifferentCompressionType) {
  options_.min_blob_size = 1024;
  options_.blob_file_compression = CompressionType::kSnappyCompression;

  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
  Flush();
  Close();

  options_.blob_file_compression = CompressionType::kLZ4Compression;
  Open();
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value, std::string(10 * 1024, 'v'));
  Close();
}

TEST_F(TitanDBTest, EmptyCompaction) {
  Open();
  CompactAll();
  Close();
}

TEST_F(TitanDBTest, SmallCompaction) {
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10, 'v')));
  ASSERT_OK(db_->Put(WriteOptions(), "k2", std::string(10, 'v')));
  Flush();
  CompactAll();
  Close();
}

TEST_F(TitanDBTest, MixCompaction) {
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(0, 'v')));
  ASSERT_OK(db_->Put(WriteOptions(), "k2", std::string(100, 'v')));
  ASSERT_OK(db_->Put(WriteOptions(), "k3", std::string(10 * 1024, 'v')));
  Flush();
  CompactAll();
  Close();
}

TEST_F(TitanDBTest, LargeCompaction) {
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
  ASSERT_OK(db_->Put(WriteOptions(), "k2", std::string(10 * 1024, 'v')));
  Flush();
  CompactAll();
  Close();
}

TEST_F(TitanDBTest, CFCompaction) {
  options_.disable_background_gc = false;
  Open();
  AddCF("cfa");
  auto cfa = cf_handles_.back();
  AddCF("cfb");
  auto cfb = cf_handles_.back();

  ASSERT_OK(db_->Put(WriteOptions(), cfa, "k1", std::string(10 * 1024, 'v')));
  ASSERT_OK(db_->Put(WriteOptions(), cfb, "k1", std::string(10 * 1024, 'v')));
  Flush(cfa);
  Flush(cfb);

  ASSERT_OK(db_->Delete(WriteOptions(), cfa, "k1"));
  ASSERT_OK(db_->Delete(WriteOptions(), cfb, "k1"));
  Flush(cfa);
  Flush(cfb);
  std::string value;
  ASSERT_TRUE(db_->GetProperty(cfa, "rocksdb.num-files-at-level0", &value));
  ASSERT_EQ(value, "2");
  db_impl_->TEST_WaitForBackgroundGC();
  CompactAll(cfa);
  // the first `CompactAll` only move files to L6 without merging the two SSTs,
  // so call it again to compact them.
  CompactAll(cfa);
  ASSERT_TRUE(db_->GetProperty(cfa, "rocksdb.num-files-at-level0", &value));
  ASSERT_EQ(value, "0");
  ASSERT_TRUE(db_->GetProperty(cfa, "rocksdb.num-files-at-level6", &value));
  ASSERT_EQ(value, "0");

  CheckBlobFileCount(0, cfa);
  CheckBlobFileCount(1, cfb);
}

TEST_F(TitanDBTest, ReAddCFCompaction) {
  options_.disable_background_gc = false;
  Open();
  AddCF("cfa");
  DropCF("cfa");

  AddCF("cfa");
  auto cfa = cf_handles_.back();
  ASSERT_OK(db_->Put(WriteOptions(), cfa, "k1", std::string(10 * 1024, 'v')));
  Flush(cfa);
  ASSERT_OK(db_->Delete(WriteOptions(), cfa, "k1"));
  Flush(cfa);
  CheckBlobFileCount(1, cfa);
  CompactAll(cfa);
  CompactAll(cfa);

  CheckBlobFileCount(0, cfa);
}

TEST_F(TitanDBTest, DeleteAllGC) {
  options_.max_background_gc = 2;
  options_.disable_background_gc = false;
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100, 'v')));
  ASSERT_OK(db_->Put(WriteOptions(), "k2", std::string(10 * 1024, 'v')));
  Flush();
  ASSERT_OK(db_->Delete(WriteOptions(), "k1"));
  ASSERT_OK(db_->Delete(WriteOptions(), "k2"));
  Flush();
  CompactAll();
  CompactAll();

  CheckBlobFileCount(0);
}

TEST_F(TitanDBTest, LowDiscardableRatio) {
  options_.max_background_gc = 2;
  options_.disable_background_gc = false;
  options_.blob_file_discardable_ratio = 0.01;
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
  auto snap = db_->GetSnapshot();
  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100 * 1024, 'v')));
  Flush();

  db_->ReleaseSnapshot(snap);
  CheckBlobFileCount(1);
  std::shared_ptr<BlobStorage> blob_storage = GetBlobStorage().lock();
  ASSERT_TRUE(blob_storage != nullptr);
  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
  blob_storage->ExportBlobFiles(blob_files);
  auto prev_file_size = blob_files.begin()->second.lock()->file_size();
  CompactAll();
  CompactAll();

  CheckBlobFileCount(1);
  blob_storage->ExportBlobFiles(blob_files);
  ASSERT_TRUE(blob_files.begin()->second.lock()->file_size() < prev_file_size);
}

TEST_F(TitanDBTest, PutDeletedDuringGC) {
  options_.max_background_gc = 2;
  options_.disable_background_gc = false;
  options_.blob_file_discardable_ratio = 0.01;
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
  auto snap = db_->GetSnapshot();
  ASSERT_OK(db_->Delete(WriteOptions(), "k1"));
  Flush();

  db_->ReleaseSnapshot(snap);
  CheckBlobFileCount(1);
  std::shared_ptr<BlobStorage> blob_storage = GetBlobStorage().lock();
  ASSERT_TRUE(blob_storage != nullptr);
  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
  blob_storage->ExportBlobFiles(blob_files);

  CheckBlobFileCount(1);
  SyncPoint::GetInstance()->LoadDependency(
      {{"TitanDBTest::PutDeletedDuringGC::ContinueGC",
        "BlobGCJob::Finish::BeforeRewriteValidKeyToLSM"},
       {"BlobGCJob::Finish::AfterRewriteValidKeyToLSM",
        "TitanDBTest::PutDeletedDuringGC::WaitGC"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CompactAll();
  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100 * 1024, 'v')));
  blob_storage->ExportBlobFiles(blob_files);
  ASSERT_EQ(blob_files.size(), 1);

  TEST_SYNC_POINT("TitanDBTest::PutDeletedDuringGC::ContinueGC");
  TEST_SYNC_POINT("TitanDBTest::PutDeletedDuringGC::WaitGC");

  CheckBlobFileCount(0);
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value, std::string(100 * 1024, 'v'));
}

TEST_F(TitanDBTest, IngestDuringGC) {
  options_.max_background_gc = 2;
  options_.disable_background_gc = false;
  options_.blob_file_discardable_ratio = 0.01;
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100, 'v')));
  auto snap = db_->GetSnapshot();
  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
  Flush();

  db_->ReleaseSnapshot(snap);
  CheckBlobFileCount(1);
  std::shared_ptr<BlobStorage> blob_storage = GetBlobStorage().lock();
  ASSERT_TRUE(blob_storage != nullptr);
  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
  blob_storage->ExportBlobFiles(blob_files);

  SyncPoint::GetInstance()->LoadDependency(
      {{"TitanDBTest::PutDeletedDuringGC::ContinueGC",
        "BlobGCJob::Finish::BeforeRewriteValidKeyToLSM"},
       {"BlobGCJob::Finish::AfterRewriteValidKeyToLSM",
        "TitanDBTest::PutDeletedDuringGC::WaitGC"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CompactAll();

  SstFileWriter sst_file_writer(EnvOptions(), options_);
  std::string sst_file = options_.dirname + "/for_ingest.sst";
  ASSERT_OK(sst_file_writer.Open(sst_file));
  ASSERT_OK(sst_file_writer.Put("k1", std::string(100 * 1024, 'v')));
  ASSERT_OK(sst_file_writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_file}, IngestExternalFileOptions()));

  blob_storage->ExportBlobFiles(blob_files);
  ASSERT_EQ(blob_files.size(), 2);

  TEST_SYNC_POINT("TitanDBTest::PutDeletedDuringGC::ContinueGC");
  TEST_SYNC_POINT("TitanDBTest::PutDeletedDuringGC::WaitGC");

  CheckBlobFileCount(1);
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value, std::string(100 * 1024, 'v'));
}

TEST_F(TitanDBTest, CompactionDuringFlush) {
  options_.max_background_gc = 1;
  options_.disable_background_gc = true;
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), "k1", "value"));
  Flush();

  SyncPoint::GetInstance()->LoadDependency(
      {{"TitanDBImpl::OnFlushCompleted:Begin1",
        "TitanDBTest::CompactionDuringFlush::WaitFlushStart"},
       {"TitanDBTest::CompactionDuringFlush::ContinueFlush",
        "TitanDBImpl::OnFlushCompleted:Begin"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
  auto snap = db_->GetSnapshot();
  ASSERT_OK(db_->Delete(WriteOptions(), "k1"));

  port::Thread writer([&]() { Flush(); });
  TEST_SYNC_POINT("TitanDBTest::CompactionDuringFlush::WaitFlushStart");
  db_->ReleaseSnapshot(snap);

  auto compact_opts = CompactRangeOptions();
  compact_opts.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_opts, nullptr, nullptr));
  ASSERT_OK(db_->CompactRange(compact_opts, nullptr, nullptr));

  TEST_SYNC_POINT("TitanDBTest::CompactionDuringFlush::ContinueFlush");
  writer.join();
  CheckBlobFileCount(1);
  SyncPoint::GetInstance()->DisableProcessing();

  std::string value;
  Status s = db_->Get(ReadOptions(), "k1", &value);
  ASSERT_TRUE(s.IsNotFound());
  // it shouldn't be any background error
  ASSERT_OK(db_->Flush(FlushOptions()));
}

TEST_F(TitanDBTest, CompactionDuringGC) {
  options_.max_background_gc = 1;
  options_.disable_background_gc = false;
  options_.blob_file_discardable_ratio = 0.01;
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
  auto snap = db_->GetSnapshot();
  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100 * 1024, 'v')));
  Flush();

  db_->ReleaseSnapshot(snap);

  SyncPoint::GetInstance()->LoadDependency(
      {{"TitanDBImpl::BackgroundGC::AfterRunGCJob",
        "TitanDBTest::CompactionDuringGC::WaitGCStart"},
       {"TitanDBTest::CompactionDuringGC::ContinueGC",
        "BlobGCJob::Finish::BeforeRewriteValidKeyToLSM"},
       {"BlobGCJob::Finish::AfterRewriteValidKeyToLSM",
        "TitanDBTest::CompactionDuringGC::WaitGCFinish"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CheckBlobFileCount(1);
  std::shared_ptr<BlobStorage> blob_storage = GetBlobStorage().lock();
  ASSERT_TRUE(blob_storage != nullptr);

  // trigger GC
  CompactAll();

  TEST_SYNC_POINT("TitanDBTest::CompactionDuringGC::WaitGCStart");

  ASSERT_OK(db_->Delete(WriteOptions(), "k1"));

  TEST_SYNC_POINT("TitanDBTest::CompactionDuringGC::ContinueGC");
  TEST_SYNC_POINT("TitanDBTest::CompactionDuringGC::WaitGCFinish");

  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
  blob_storage->ExportBlobFiles(blob_files);
  // rewriting index to LSM failed, but the output blob file is already
  // generated
  ASSERT_EQ(blob_files.size(), 2);

  std::string value;
  Status status = db_->Get(ReadOptions(), "k1", &value);
  ASSERT_EQ(status, Status::NotFound());

  SyncPoint::GetInstance()->DisableProcessing();
  CheckBlobFileCount(1);

  Flush();
  CompactAll();

  db_impl_->TEST_StartGC(db_impl_->DefaultColumnFamily()->GetID());
  CheckBlobFileCount(0);
}

TEST_F(TitanDBTest, DeleteFilesInRangeDuringGC) {
  options_.max_background_gc = 1;
  options_.disable_background_gc = false;
  options_.blob_file_discardable_ratio = 0.01;
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
  auto snap = db_->GetSnapshot();
  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100 * 1024, 'v')));
  Flush();

  db_->ReleaseSnapshot(snap);

  SyncPoint::GetInstance()->LoadDependency(
      {{"TitanDBImpl::BackgroundGC::BeforeRunGCJob",
        "TitanDBTest::DeleteFilesInRangeDuringGC::WaitGCStart"},
       {"TitanDBTest::DeleteFilesInRangeDuringGC::ContinueGC",
        "BlobGCJob::Finish::BeforeRewriteValidKeyToLSM"},
       {"BlobGCJob::Finish::AfterRewriteValidKeyToLSM",
        "TitanDBTest::DeleteFilesInRangeDuringGC::WaitGCFinish"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CheckBlobFileCount(1);

  // trigger GC
  CompactAll();

  TEST_SYNC_POINT("TitanDBTest::DeleteFilesInRangeDuringGC::WaitGCStart");
  DeleteFilesInRange(nullptr, nullptr);

  TEST_SYNC_POINT("TitanDBTest::DeleteFilesInRangeDuringGC::ContinueGC");
  TEST_SYNC_POINT("TitanDBTest::DeleteFilesInRangeDuringGC::WaitGCFinish");

  std::string value;
  Status s = db_->Get(ReadOptions(), "k1", &value);
  ASSERT_TRUE(s.IsNotFound());
  // it shouldn't be any background error
  ASSERT_OK(db_->Flush(FlushOptions()));

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(TitanDBTest, Config) {
  options_.disable_background_gc = false;

  options_.max_background_gc = 0;
  Open();
  options_.max_background_gc = 1;
  Reopen();
  options_.max_background_gc = 2;
  Reopen();

  options_.min_blob_size = 512;
  Reopen();
  options_.min_blob_size = 1024;
  Reopen();
  options_.min_blob_size = 64 * 1024 * 1024;
  Reopen();

  options_.blob_file_discardable_ratio = 0;
  Reopen();
  options_.blob_file_discardable_ratio = 0.001;
  Reopen();
  options_.blob_file_discardable_ratio = 1;

  options_.blob_cache = NewLRUCache(0);
  Reopen();
  options_.blob_cache = NewLRUCache(1 * 1024 * 1024);
  Reopen();

  Close();
}

#if defined(__linux) && !defined(TRAVIS)
TEST_F(TitanDBTest, DISABLED_NoSpaceLeft) {
  options_.disable_background_gc = false;
  system(("mkdir -p " + dbname_).c_str());
  system(("sudo mount -t tmpfs -o size=1m tmpfs " + dbname_).c_str());
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100 * 1024, 'v')));
  ASSERT_OK(db_->Put(WriteOptions(), "k2", std::string(100 * 1024, 'v')));
  ASSERT_OK(db_->Put(WriteOptions(), "k3", std::string(100 * 1024, 'v')));
  Flush();
  ASSERT_OK(db_->Put(WriteOptions(), "k4", std::string(300 * 1024, 'v')));
  ASSERT_NOK(db_->Flush(FlushOptions()));

  Close();
  system(("sudo umount -l " + dbname_).c_str());
}
#endif

TEST_F(TitanDBTest, RecoverAfterCrash) {
  const uint64_t kNumKeys = 100;
  std::map<std::string, std::string> data;
  Open();
  for (uint64_t k = 1; k <= kNumKeys; k++) {
    Put(k, &data);
  }
  Flush();
  auto new_fn = db_impl_->TEST_GetBlobFileSet()->NewFileNumber();
  auto name = BlobFileName(options_.dirname, new_fn);
  std::unique_ptr<WritableFileWriter> file;
  {
    std::unique_ptr<FSWritableFile> f;
    ASSERT_OK(env_->GetFileSystem()->NewWritableFile(name, FileOptions(), &f,
                                                     nullptr /*dbg*/));
  }
  Close();
  ASSERT_OK(env_->GetFileSystem()->FileExists(name, IOOptions(), nullptr));
  // During reopen, the recovery process should delete the file.
  Open();
  Close();
  ASSERT_NOK(env_->GetFileSystem()->FileExists(name, IOOptions(), nullptr));
}

TEST_F(TitanDBTest, OnlineChangeMinBlobSize) {
  const uint64_t kNumKeys = 100;
  std::map<std::string, std::string> data;
  Open();
  for (uint64_t k = 1; k <= kNumKeys; k++) {
    Put(k, &data);
  }
  Flush();
  auto blob_storage = GetBlobStorage(db_->DefaultColumnFamily());
  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
  blob_storage.lock()->ExportBlobFiles(blob_files);
  ASSERT_EQ(1, blob_files.size());
  auto blob_file = blob_files.begin();
  VerifyBlob(blob_file->first, data);
  auto first_blob_file_number = blob_file->first;

  std::unordered_map<std::string, std::string> opts = {
      {"min_blob_size", "123"}};
  ASSERT_OK(db_->SetOptions(opts));
  auto titan_options = db_->GetTitanOptions();
  ASSERT_EQ(123, titan_options.min_blob_size);
  options_.min_blob_size = 123;

  data.clear();
  for (uint64_t k = 1; k <= kNumKeys; k++) {
    Put(k, &data);
  }
  Flush();
  blob_files.clear();
  blob_storage.lock()->ExportBlobFiles(blob_files);
  ASSERT_EQ(2, blob_files.size());
  for (const auto& pair : blob_files) {
    if (pair.first != first_blob_file_number) {
      VerifyBlob(pair.first, data);
    }
  }
}

TEST_F(TitanDBTest, OnlineChangeCompressionType) {
#ifdef LZ4
  const uint64_t kNumKeys = 100;
  std::map<std::string, std::string> data;
  Open();

  std::unordered_map<std::string, std::string> opts = {
      {"blob_file_compression", "kNoCompression"}};
  ASSERT_OK(db_->SetOptions(opts));
  auto titan_options = db_->GetTitanOptions();
  ASSERT_EQ(kNoCompression, titan_options.blob_file_compression);
  for (uint64_t k = 1; k <= kNumKeys; k++) {
    Put(k, &data);
  }
  Flush();
  auto blob_storage = GetBlobStorage(db_->DefaultColumnFamily());
  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
  blob_storage.lock()->ExportBlobFiles(blob_files);
  ASSERT_EQ(1, blob_files.size());
  auto blob_file = blob_files.begin();
  VerifyBlob(blob_file->first, data);
  auto first_blob_file_number = blob_file->first;
  auto first_blob_file_size = blob_file->second.lock()->file_size();

  data.clear();
  for (uint64_t k = 1; k <= kNumKeys; k++) {
    Put(k, &data);
  }
  Flush();
  blob_files.clear();
  blob_storage.lock()->ExportBlobFiles(blob_files);
  ASSERT_EQ(2, blob_files.size());
  uint64_t second_blob_file_number = 0;
  for (const auto& pair : blob_files) {
    if (pair.first != first_blob_file_number) {
      second_blob_file_number = pair.first;
      ASSERT_EQ(first_blob_file_size, pair.second.lock()->file_size());
      VerifyBlob(pair.first, data);
    }
  }

  opts = {{"blob_file_compression", "kLZ4Compression"}};
  ASSERT_OK(db_->SetOptions(opts));
  titan_options = db_->GetTitanOptions();
  ASSERT_EQ(kLZ4Compression, titan_options.blob_file_compression);

  data.clear();
  for (uint64_t k = 1; k <= kNumKeys; k++) {
    Put(k, &data);
  }
  Flush();
  blob_files.clear();
  blob_storage.lock()->ExportBlobFiles(blob_files);
  ASSERT_EQ(3, blob_files.size());
  for (const auto& pair : blob_files) {
    if (pair.first != first_blob_file_number &&
        pair.first != second_blob_file_number) {
      VerifyBlob(pair.first, data);
      // The third blob file should be smaller, since it uses compression
      // algorithm.
      ASSERT_GT(first_blob_file_size, pair.second.lock()->file_size());
    }
  }
#endif
}

TEST_F(TitanDBTest, OnlineChangeBlobFileDiscardableRatio) {
  options_.min_blob_size = 0;
  const uint64_t kNumKeys = 100;
  std::map<std::string, std::string> data;
  Open();
  uint32_t default_cf_id = db_->DefaultColumnFamily()->GetID();

  std::unordered_map<std::string, std::string> opts = {
      {"blob_file_discardable_ratio", "0.8"}};
  ASSERT_OK(db_->SetOptions(opts));
  for (uint64_t k = 1; k <= kNumKeys; k++) {
    auto key = GenKey(k);
    ASSERT_OK(db_->Put(WriteOptions(), key, "v"));
  }
  Flush();

  data.clear();
  for (uint64_t k = 1; k <= kNumKeys; k++) {
    if (k % 2 == 0) {
      Delete(k);
    }
  }
  Flush();
  CompactAll();

  db_impl_->TEST_StartGC(default_cf_id);
  db_impl_->TEST_WaitForBackgroundGC();

  auto blob_storage = GetBlobStorage(db_->DefaultColumnFamily());
  ASSERT_EQ(blob_storage.lock()->cf_options().blob_file_discardable_ratio, 0.8);
  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
  blob_storage.lock()->ExportBlobFiles(blob_files);
  ASSERT_EQ(1, blob_files.size());
  auto blob_file = blob_files.begin();
  ASSERT_GT(blob_file->second.lock()->GetDiscardableRatio(), 0.4);

  opts = {{"blob_file_discardable_ratio", "0.4"}};
  ASSERT_OK(db_->SetOptions(opts));
  auto titan_options = db_->GetTitanOptions();
  ASSERT_EQ(0.4, titan_options.blob_file_discardable_ratio);

  db_impl_->TEST_StartGC(default_cf_id);
  db_impl_->TEST_WaitForBackgroundGC();
  ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());

  blob_files.clear();
  blob_storage.lock()->ExportBlobFiles(blob_files);
  ASSERT_EQ(1, blob_files.size());
  blob_file = blob_files.begin();
  // The discardable ratio should be updated after GC.
  ASSERT_LT(blob_file->second.lock()->GetDiscardableRatio(), 0.4);
}
}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
