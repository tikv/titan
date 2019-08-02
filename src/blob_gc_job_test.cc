#include "blob_gc_job.h"

#include "blob_gc_picker.h"
#include "db_impl.h"
#include "rocksdb/convenience.h"
#include "util/testharness.h"

namespace rocksdb {
namespace titandb {

const static int MAX_KEY_NUM = 1000;

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

class BlobGCJobTest : public testing::Test {
 public:
  std::string dbname_;
  TitanDB* db_;
  DBImpl* base_db_;
  TitanDBImpl* tdb_;
  VersionSet* version_set_;
  TitanOptions options_;
  port::Mutex* mutex_;

  BlobGCJobTest() : dbname_(test::TmpDir()) {
    options_.dirname = dbname_ + "/titandb";
    options_.create_if_missing = true;
    options_.disable_background_gc = true;
    options_.min_blob_size = 0;
    options_.disable_auto_compactions = true;
    options_.env->CreateDirIfMissing(dbname_);
    options_.env->CreateDirIfMissing(options_.dirname);
  }
  ~BlobGCJobTest() {}

  std::weak_ptr<BlobStorage> GetBlobStorage(uint32_t cf_id) {
    MutexLock l(mutex_);
    return version_set_->GetBlobStorage(cf_id);
  }

  void CheckBlobNumber(int expected) {
    auto b = GetBlobStorage(base_db_->DefaultColumnFamily()->GetID()).lock();
    ASSERT_EQ(expected, b->files_.size());
  }

  void ClearDir() {
    std::vector<std::string> filenames;
    options_.env->GetChildren(options_.dirname, &filenames);
    for (auto& fname : filenames) {
      if (fname != "." && fname != "..") {
        ASSERT_OK(options_.env->DeleteFile(options_.dirname + "/" + fname));
      }
    }
    options_.env->DeleteDir(options_.dirname);
    filenames.clear();
    options_.env->GetChildren(dbname_, &filenames);
    for (auto& fname : filenames) {
      if (fname != "." && fname != "..") {
        options_.env->DeleteFile(dbname_ + "/" + fname);
      }
    }
  }

  void NewDB() {
    ClearDir();
    ASSERT_OK(TitanDB::Open(options_, dbname_, &db_));
    tdb_ = reinterpret_cast<TitanDBImpl*>(db_);
    version_set_ = tdb_->vset_.get();
    mutex_ = &tdb_->mutex_;
    base_db_ = reinterpret_cast<DBImpl*>(tdb_->GetRootDB());
  }

  void Flush() {
    FlushOptions fopts;
    fopts.wait = true;
    ASSERT_OK(db_->Flush(fopts));
  }

  void CompactAll() {
    auto opts = db_->GetOptions();
    auto compact_opts = CompactRangeOptions();
    compact_opts.change_level = true;
    compact_opts.target_level = opts.num_levels - 1;
    compact_opts.bottommost_level_compaction = BottommostLevelCompaction::kSkip;
    ASSERT_OK(db_->CompactRange(compact_opts, nullptr, nullptr));
  }

  void DestroyDB() {
    Status s __attribute__((__unused__)) = db_->Close();
    assert(s.ok());
    delete db_;
    db_ = nullptr;
  }

  void TriggerGC() {
    assert(db_);
    for (int i = 0; i < MAX_KEY_NUM; i++) {
      db_->Put(WriteOptions(), GenKey(i), GenValue(i));
    }
    Flush();
    for (int i = 0; i < MAX_KEY_NUM; i++) {
      db_->Put(WriteOptions(), GenKey(i), GenValue(i));
    }
    Flush();
    RunGC();
  }

  void RunGC(bool expected = false) {
    MutexLock l(mutex_);
    Status s;
    auto* cfh = base_db_->DefaultColumnFamily();

    // Build BlobGC
    TitanDBOptions db_options;
    TitanCFOptions cf_options;
    LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options.info_log.get());
    cf_options.min_gc_batch_size = 0;
    cf_options.blob_file_discardable_ratio = 0.4;
    cf_options.sample_file_size_ratio = 1;

    std::unique_ptr<BlobGC> blob_gc;
    {
      std::shared_ptr<BlobGCPicker> blob_gc_picker =
          std::make_shared<BasicBlobGCPicker>(db_options, cf_options);
      blob_gc = blob_gc_picker->PickBlobGC(
          version_set_->GetBlobStorage(cfh->GetID()).lock().get());
    }

    if (expected) {
      ASSERT_TRUE(blob_gc != nullptr);
    }

    if (blob_gc) {
      blob_gc->SetColumnFamily(cfh);

      BlobGCJob blob_gc_job(blob_gc.get(), base_db_, mutex_, tdb_->db_options_,
                            tdb_->env_, EnvOptions(options_),
                            tdb_->blob_manager_.get(), version_set_,
                            &log_buffer, nullptr, nullptr);

      s = blob_gc_job.Prepare();
      ASSERT_OK(s);

      {
        mutex_->Unlock();
        s = blob_gc_job.Run();
        if (expected) {
          ASSERT_OK(s);
        }
        mutex_->Lock();
      }

      if (s.ok()) {
        s = blob_gc_job.Finish();
        ASSERT_OK(s);
      }
    }

    mutex_->Unlock();
    tdb_->PurgeObsoleteFiles();
    mutex_->Lock();
  }

  Status NewIterator(uint64_t file_number, uint64_t file_size,
                     std::unique_ptr<BlobFileIterator>* iter) {
    std::unique_ptr<RandomAccessFileReader> file;
    Status s = NewBlobFileReader(file_number, 0, tdb_->db_options_,
                                 tdb_->env_options_, tdb_->env_, &file);
    if (!s.ok()) {
      return s;
    }
    iter->reset(new BlobFileIterator(std::move(file), file_number, file_size,
                                     TitanCFOptions()));
    return Status::OK();
  }

  void TestDiscardEntry() {
    NewDB();
    auto* cfh = base_db_->DefaultColumnFamily();
    BlobIndex blob_index;
    blob_index.file_number = 0x81;
    blob_index.blob_handle.offset = 0x98;
    blob_index.blob_handle.size = 0x17;
    std::string res;
    blob_index.EncodeTo(&res);
    std::string key = "test_discard_entry";
    WriteBatch wb;
    ASSERT_OK(WriteBatchInternal::PutBlobIndex(&wb, cfh->GetID(), key, res));
    auto rewrite_status = base_db_->Write(WriteOptions(), &wb);

    std::vector<BlobFileMeta*> tmp;
    BlobGC blob_gc(std::move(tmp), TitanCFOptions(), false /*trigger_next*/);
    blob_gc.SetColumnFamily(cfh);
    BlobGCJob blob_gc_job(&blob_gc, base_db_, mutex_, TitanDBOptions(),
                          Env::Default(), EnvOptions(), nullptr, version_set_,
                          nullptr, nullptr, nullptr);
    bool discardable = false;
    ASSERT_OK(blob_gc_job.DiscardEntry(key, blob_index, &discardable));
    ASSERT_FALSE(discardable);
    DestroyDB();
  }

  void TestGCLimiter() {
    class TestLimiter : public RateLimiter {
     public:
      TestLimiter(RateLimiter::Mode mode)
          : RateLimiter(mode), read(false), write(false) {}

      virtual size_t RequestToken(size_t bytes, size_t alignment,
                                  Env::IOPriority io_priority,
                                  Statistics* stats,
                                  RateLimiter::OpType op_type) {
        if (IsRateLimited(op_type)) {
          if (op_type == RateLimiter::OpType::kRead) {
            read = true;
          } else {
            write = true;
          }
        }
        return bytes;
      }

      virtual void SetBytesPerSecond(int64_t bytes_per_second) {}

      virtual int64_t GetSingleBurstBytes() const { return 0; }

      virtual int64_t GetTotalBytesThrough(
          const Env::IOPriority pri = Env::IO_TOTAL) const {
        return 0;
      }

      virtual int64_t GetTotalRequests(
          const Env::IOPriority pri = Env::IO_TOTAL) const {
        return 0;
      }

      virtual int64_t GetBytesPerSecond() const { return 0; }

      bool ReadRequested() { return read; }

      bool WriteRequested() { return write; }

     private:
      bool read;
      bool write;
    };

    TestLimiter* test_limiter = new TestLimiter(RateLimiter::Mode::kWritesOnly);
    options_.rate_limiter = std::shared_ptr<RateLimiter>(test_limiter);
    NewDB();
    TriggerGC();
    ASSERT_TRUE(test_limiter->WriteRequested());
    ASSERT_FALSE(test_limiter->ReadRequested());
    DestroyDB();

    test_limiter = new TestLimiter(RateLimiter::Mode::kReadsOnly);
    options_.rate_limiter.reset(test_limiter);
    NewDB();
    TriggerGC();
    ASSERT_FALSE(test_limiter->WriteRequested());
    ASSERT_TRUE(test_limiter->ReadRequested());
    DestroyDB();

    test_limiter = new TestLimiter(RateLimiter::Mode::kAllIo);
    options_.rate_limiter.reset(test_limiter);
    NewDB();
    TriggerGC();
    ASSERT_TRUE(test_limiter->WriteRequested());
    ASSERT_TRUE(test_limiter->ReadRequested());
    DestroyDB();
  }

  void TestRunGC() {
    NewDB();
    for (int i = 0; i < MAX_KEY_NUM; i++) {
      db_->Put(WriteOptions(), GenKey(i), GenValue(i));
    }
    Flush();
    std::string result;
    for (int i = 0; i < MAX_KEY_NUM; i++) {
      if (i % 2 != 0) continue;
      db_->Delete(WriteOptions(), GenKey(i));
    }
    Flush();
    auto b = GetBlobStorage(base_db_->DefaultColumnFamily()->GetID()).lock();
    ASSERT_EQ(b->files_.size(), 1);
    auto old = b->files_.begin()->first;
    //    for (auto& f : b->files_) {
    //      f.second->marked_for_sample = false;
    //    }
    std::unique_ptr<BlobFileIterator> iter;
    ASSERT_OK(NewIterator(b->files_.begin()->second->file_number(),
                          b->files_.begin()->second->file_size(), &iter));
    iter->SeekToFirst();
    for (int i = 0; i < MAX_KEY_NUM; i++, iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(iter->key().compare(Slice(GenKey(i))) == 0);
    }
    RunGC();
    b = GetBlobStorage(base_db_->DefaultColumnFamily()->GetID()).lock();
    ASSERT_EQ(b->files_.size(), 1);
    auto new1 = b->files_.begin()->first;
    ASSERT_TRUE(old != new1);
    ASSERT_OK(NewIterator(b->files_.begin()->second->file_number(),
                          b->files_.begin()->second->file_size(), &iter));
    iter->SeekToFirst();
    auto* db_iter = db_->NewIterator(ReadOptions(), db_->DefaultColumnFamily());
    db_iter->SeekToFirst();
    for (int i = 0; i < MAX_KEY_NUM; i++) {
      if (i % 2 == 0) continue;
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(iter->key().compare(Slice(GenKey(i))) == 0);
      ASSERT_TRUE(iter->value().compare(Slice(GenValue(i))) == 0);
      ASSERT_OK(db_->Get(ReadOptions(), iter->key(), &result));
      ASSERT_TRUE(iter->value().size() == result.size());
      ASSERT_TRUE(iter->value().compare(result) == 0);

      ASSERT_OK(db_iter->status());
      ASSERT_TRUE(db_iter->Valid());
      ASSERT_TRUE(db_iter->key().compare(Slice(GenKey(i))) == 0);
      ASSERT_TRUE(db_iter->value().compare(Slice(GenValue(i))) == 0);
      iter->Next();
      db_iter->Next();
    }
    delete db_iter;
    ASSERT_FALSE(iter->Valid() || !iter->status().ok());
    DestroyDB();
  }
};

TEST_F(BlobGCJobTest, DiscardEntry) { TestDiscardEntry(); }

TEST_F(BlobGCJobTest, RunGC) { TestRunGC(); }

TEST_F(BlobGCJobTest, GCLimiter) { TestGCLimiter(); }

// Tests blob file will be kept after GC, if it is still visible by active
// snapshots.
TEST_F(BlobGCJobTest, PurgeBlobs) {
  NewDB();

  auto snap1 = db_->GetSnapshot();

  for (int i = 0; i < 10; i++) {
    db_->Put(WriteOptions(), GenKey(i), GenValue(i));
  }
  Flush();
  CheckBlobNumber(1);
  auto snap2 = db_->GetSnapshot();
  auto snap3 = db_->GetSnapshot();

  for (int i = 0; i < 10; i++) {
    db_->Delete(WriteOptions(), GenKey(i));
  }
  Flush();
  CheckBlobNumber(1);
  auto snap4 = db_->GetSnapshot();

  RunGC();
  CheckBlobNumber(1);

  for (int i = 10; i < 20; i++) {
    db_->Put(WriteOptions(), GenKey(i), GenValue(i));
  }
  Flush();
  auto snap5 = db_->GetSnapshot();
  CheckBlobNumber(2);

  db_->ReleaseSnapshot(snap2);
  RunGC();
  CheckBlobNumber(3);

  db_->ReleaseSnapshot(snap3);
  RunGC();
  CheckBlobNumber(3);

  db_->ReleaseSnapshot(snap1);
  RunGC();
  CheckBlobNumber(3);

  db_->ReleaseSnapshot(snap4);
  RunGC();
  CheckBlobNumber(2);

  db_->ReleaseSnapshot(snap5);
  RunGC();
  CheckBlobNumber(1);

  DestroyDB();
}

TEST_F(BlobGCJobTest, DeleteFilesInRange) {
  NewDB();

  ASSERT_OK(db_->Put(WriteOptions(), GenKey(2), GenValue(21)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(4), GenValue(4)));
  Flush();
  CompactAll();
  std::string value;
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &value));
  ASSERT_EQ(value, "0");
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level6", &value));
  ASSERT_EQ(value, "1");

  SstFileWriter sst_file_writer(EnvOptions(), options_);
  std::string sst_file = options_.dirname + "/for_ingest.sst";
  ASSERT_OK(sst_file_writer.Open(sst_file));
  ASSERT_OK(sst_file_writer.Put(GenKey(1), GenValue(1)));
  ASSERT_OK(sst_file_writer.Put(GenKey(2), GenValue(22)));
  ASSERT_OK(sst_file_writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_file}, IngestExternalFileOptions()));
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &value));
  ASSERT_EQ(value, "0");
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level5", &value));
  ASSERT_EQ(value, "1");
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level6", &value));
  ASSERT_EQ(value, "1");

  RunGC(true);

  std::string key0 = GenKey(0);
  std::string key3 = GenKey(3);
  Slice start = Slice(key0);
  Slice end = Slice(key3);
  ASSERT_OK(
      DeleteFilesInRange(base_db_, db_->DefaultColumnFamily(), &start, &end));
  TitanReadOptions opts;
  auto* iter = db_->NewIterator(opts, db_->DefaultColumnFamily());
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->Next();
  }
  ASSERT_TRUE(iter->status().IsCorruption());
  delete iter;

  opts.key_only = true;
  iter = db_->NewIterator(opts, db_->DefaultColumnFamily());
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->Next();
  }
  ASSERT_OK(iter->status());
  delete iter;

  DestroyDB();
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
