#include "blob_gc_job.h"

#include "blob_gc_picker.h"
#include "db_impl.h"
#include "rocksdb/convenience.h"
#include "test_util/testharness.h"

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

class BlobGCJobTest : public testing::TestWithParam<bool /*gc_merge_mode*/> {
 public:
  std::string dbname_;
  TitanDB* db_;
  DBImpl* base_db_;
  TitanDBImpl* tdb_;
  BlobFileSet* blob_file_set_;
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

  ~BlobGCJobTest() { Close(); }

  void DisableMergeSmall() { options_.merge_small_file_threshold = 0; }

  std::weak_ptr<BlobStorage> GetBlobStorage(uint32_t cf_id) {
    MutexLock l(mutex_);
    return blob_file_set_->GetBlobStorage(cf_id);
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
    Open();
  }

  void Open() {
    ASSERT_OK(TitanDB::Open(options_, dbname_, &db_));
    tdb_ = reinterpret_cast<TitanDBImpl*>(db_);
    blob_file_set_ = tdb_->blob_file_set_.get();
    mutex_ = &tdb_->mutex_;
    base_db_ = reinterpret_cast<DBImpl*>(tdb_->GetRootDB());
  }

  void Reopen() {
    Close();
    Open();
  }

  void ScheduleRangeMerge(
      const std::vector<std::shared_ptr<BlobFileMeta>>& files,
      int max_sorted_runs) {
    tdb_->mutex_.Lock();
    tdb_->MarkFileIfNeedMerge(files, max_sorted_runs);
    tdb_->mutex_.Unlock();
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

  void ReComputeGCScore() {
    auto b = GetBlobStorage(base_db_->DefaultColumnFamily()->GetID()).lock();
    b->ComputeGCScore();
  }

  void Close() {
    if (!db_) return;
    ASSERT_OK(db_->Close());
    delete db_;
    db_ = nullptr;
  }

  // TODO: unifiy this and TitanDBImpl::TEST_StartGC
  void RunGC(bool expect_gc, bool disable_merge_small = false) {
    MutexLock l(mutex_);
    Status s;
    auto* cfh = base_db_->DefaultColumnFamily();

    // Build BlobGC
    TitanDBOptions db_options;
    TitanCFOptions cf_options;
    LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options.info_log.get());
    cf_options.min_gc_batch_size = 0;
    if (disable_merge_small) {
      cf_options.merge_small_file_threshold = 0;
    }
    cf_options.blob_file_discardable_ratio = 0.4;
    cf_options.sample_file_size_ratio = 1;

    std::unique_ptr<BlobGC> blob_gc;
    {
      std::shared_ptr<BlobGCPicker> blob_gc_picker =
          std::make_shared<BasicBlobGCPicker>(db_options, cf_options, nullptr);
      blob_gc = blob_gc_picker->PickBlobGC(
          blob_file_set_->GetBlobStorage(cfh->GetID()).lock().get());
    }

    ASSERT_TRUE((blob_gc != nullptr) == expect_gc);

    if (blob_gc) {
      blob_gc->SetColumnFamily(cfh);

      BlobGCJob blob_gc_job(blob_gc.get(), base_db_, mutex_, tdb_->db_options_,
                            GetParam(), tdb_->env_, EnvOptions(options_),
                            tdb_->blob_manager_.get(), blob_file_set_,
                            &log_buffer, nullptr, nullptr);

      s = blob_gc_job.Prepare();
      ASSERT_OK(s);

      {
        mutex_->Unlock();
        s = blob_gc_job.Run();
        if (expect_gc) {
          ASSERT_OK(s);
        }
        mutex_->Lock();
      }

      if (s.ok()) {
        s = blob_gc_job.Finish();
        ASSERT_OK(s);
      }
      blob_gc->ReleaseGcFiles();
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

    std::vector<std::shared_ptr<BlobFileMeta>> tmp;
    BlobGC blob_gc(std::move(tmp), TitanCFOptions(), false /*trigger_next*/);
    blob_gc.SetColumnFamily(cfh);
    BlobGCJob blob_gc_job(&blob_gc, base_db_, mutex_, TitanDBOptions(),
                          GetParam(), Env::Default(), EnvOptions(), nullptr,
                          blob_file_set_, nullptr, nullptr, nullptr);
    bool discardable = false;
    ASSERT_OK(blob_gc_job.DiscardEntry(key, blob_index, &discardable));
    ASSERT_FALSE(discardable);
  }

  void TestRunGC() {
    NewDB();
    for (int i = 0; i < MAX_KEY_NUM; i++) {
      db_->Put(WriteOptions(), GenKey(i), GenValue(i));
    }
    Flush();
    std::string result;
    for (int i = 0; i < MAX_KEY_NUM; i++) {
      if (i % 3 == 0) continue;
      db_->Delete(WriteOptions(), GenKey(i));
    }
    Flush();
    CompactAll();
    auto b = GetBlobStorage(base_db_->DefaultColumnFamily()->GetID()).lock();
    ASSERT_EQ(b->files_.size(), 1);
    auto old = b->files_.begin()->first;
    std::unique_ptr<BlobFileIterator> iter;
    ASSERT_OK(NewIterator(b->files_.begin()->second->file_number(),
                          b->files_.begin()->second->file_size(), &iter));
    iter->SeekToFirst();
    for (int i = 0; i < MAX_KEY_NUM; i++, iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(iter->key().compare(Slice(GenKey(i))) == 0);
    }
    RunGC(true);
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
      if (i % 3 != 0) continue;
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
  }
};

TEST_P(BlobGCJobTest, DiscardEntry) { TestDiscardEntry(); }

TEST_P(BlobGCJobTest, RunGC) { TestRunGC(); }

TEST_P(BlobGCJobTest, GCLimiter) {
  class TestLimiter : public RateLimiter {
   public:
    TestLimiter(RateLimiter::Mode mode)
        : RateLimiter(mode), read(false), write(false) {}

    size_t RequestToken(size_t bytes, size_t alignment,
                        Env::IOPriority io_priority, Statistics* stats,
                        RateLimiter::OpType op_type) override {
      // Just the same condition with the rocksdb's RequestToken
      if (io_priority < Env::IO_TOTAL && IsRateLimited(op_type)) {
        if (op_type == RateLimiter::OpType::kRead) {
          read = true;
        } else {
          write = true;
        }
      }
      return bytes;
    }

    void SetBytesPerSecond(int64_t bytes_per_second) override {}

    int64_t GetSingleBurstBytes() const override { return 0; }

    int64_t GetTotalBytesThrough(
        const Env::IOPriority pri = Env::IO_TOTAL) const override {
      return 0;
    }

    int64_t GetTotalRequests(
        const Env::IOPriority pri = Env::IO_TOTAL) const override {
      return 0;
    }

    int64_t GetBytesPerSecond() const override { return 0; }

    void Reset() {
      read = false;
      write = false;
    }

    bool ReadRequested() { return read; }

    bool WriteRequested() { return write; }

   private:
    bool read;
    bool write;
  };

  auto PutAndUpdate = [this] {
    assert(db_);
    for (int i = 0; i < MAX_KEY_NUM; i++) {
      db_->Put(WriteOptions(), GenKey(i), GenValue(i));
    }
    Flush();
    for (int i = 0; i < MAX_KEY_NUM; i++) {
      db_->Put(WriteOptions(), GenKey(i), GenValue(i));
    }
    Flush();
  };

  TestLimiter* test_limiter = new TestLimiter(RateLimiter::Mode::kWritesOnly);
  options_.rate_limiter = std::shared_ptr<RateLimiter>(test_limiter);
  NewDB();
  PutAndUpdate();
  test_limiter->Reset();
  RunGC(true);
  ASSERT_TRUE(test_limiter->WriteRequested());
  ASSERT_FALSE(test_limiter->ReadRequested());
  Close();

  test_limiter = new TestLimiter(RateLimiter::Mode::kReadsOnly);
  options_.rate_limiter.reset(test_limiter);
  NewDB();
  PutAndUpdate();
  test_limiter->Reset();
  RunGC(true);
  ASSERT_FALSE(test_limiter->WriteRequested());
  ASSERT_TRUE(test_limiter->ReadRequested());
  Close();

  test_limiter = new TestLimiter(RateLimiter::Mode::kAllIo);
  options_.rate_limiter.reset(test_limiter);
  NewDB();
  PutAndUpdate();
  test_limiter->Reset();
  RunGC(true);
  ASSERT_TRUE(test_limiter->WriteRequested());
  ASSERT_TRUE(test_limiter->ReadRequested());
  Close();
}

TEST_P(BlobGCJobTest, Reopen) {
  DisableMergeSmall();
  NewDB();
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(i), GenValue(i)));
  }
  Flush();
  CheckBlobNumber(1);

  Reopen();
  RunGC(false /*expect_gc*/, true /*disable_merge_small*/);
  CheckBlobNumber(1);
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(db_->Delete(WriteOptions(), GenKey(i)));
  }
  Flush();
  CompactAll();
  CheckBlobNumber(1);

  // Should recover GC stats after reopen.
  Reopen();
  RunGC(true /*expect_gc*/, true /*dissable_merge_small*/);
  CheckBlobNumber(1);
}

// Tests blob file will be kept after GC, if it is still visible by active
// snapshots.
TEST_P(BlobGCJobTest, PurgeBlobs) {
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

  for (int i = 10; i < 20; i++) {
    db_->Put(WriteOptions(), GenKey(i), GenValue(i));
  }
  Flush();
  CheckBlobNumber(2);

  // merge two blob files into one
  CompactAll();
  RunGC(true);
  CheckBlobNumber(3);

  auto snap5 = db_->GetSnapshot();

  db_->ReleaseSnapshot(snap2);
  RunGC(false);
  CheckBlobNumber(3);

  db_->ReleaseSnapshot(snap3);
  RunGC(false);
  CheckBlobNumber(3);

  db_->ReleaseSnapshot(snap1);
  RunGC(false);
  CheckBlobNumber(3);

  db_->ReleaseSnapshot(snap4);
  RunGC(false);
  CheckBlobNumber(3);

  db_->ReleaseSnapshot(snap5);

  RunGC(false);
  CheckBlobNumber(1);
}

TEST_P(BlobGCJobTest, DeleteFilesInRange) {
  NewDB();

  ASSERT_OK(db_->Put(WriteOptions(), GenKey(2), GenValue(21)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(4), GenValue(4)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(5), GenValue(5)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(6), GenValue(5)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(7), GenValue(5)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(8), GenValue(5)));
  ASSERT_OK(db_->Put(WriteOptions(), GenKey(9), GenValue(5)));
  Flush();
  CompactAll();
  std::string value;
  // Now the LSM structure is:
  // L6: [2, 4]
  // with 1 alive blob file
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &value));
  ASSERT_EQ(value, "0");
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level6", &value));
  ASSERT_EQ(value, "1");

  ASSERT_OK(db_->Delete(WriteOptions(), GenKey(5)));
  ASSERT_OK(db_->Delete(WriteOptions(), GenKey(6)));
  ASSERT_OK(db_->Delete(WriteOptions(), GenKey(7)));
  ASSERT_OK(db_->Delete(WriteOptions(), GenKey(8)));
  ASSERT_OK(db_->Delete(WriteOptions(), GenKey(9)));
  CompactAll();

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
  // Now the LSM structure is:
  // L5: [1, 2]
  // L6: [2, 4]
  // with 1 alive blob file
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &value));
  ASSERT_EQ(value, "0");
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level5", &value));
  ASSERT_EQ(value, "1");
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level6", &value));
  ASSERT_EQ(value, "1");

  // GC and purge blob file
  // Now the LSM structure is:
  // L5: [1, 2]
  // L6: [2, 4]
  // with 0 blob file
  CheckBlobNumber(1);

  RunGC(true);

  std::string key0 = GenKey(0);
  std::string key3 = GenKey(3);
  Slice start = Slice(key0);
  Slice end = Slice(key3);
  ASSERT_OK(
      DeleteFilesInRange(base_db_, db_->DefaultColumnFamily(), &start, &end));
  // Now the LSM structure is:
  // L6: [2, 4]
  // with 0 blob file

  TitanReadOptions opts;
  auto* iter = db_->NewIterator(opts, db_->DefaultColumnFamily());
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->Next();
  }
  // `DeleteFilesInRange` may expose old blob index.
  ASSERT_TRUE(iter->status().IsCorruption());
  delete iter;

  // Set key only to ignore the stale blob indexes.
  opts.key_only = true;
  iter = db_->NewIterator(opts, db_->DefaultColumnFamily());
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->Next();
  }
  ASSERT_OK(iter->status());
  delete iter;
}

TEST_P(BlobGCJobTest, LevelMergeGC) {
  options_.level_merge = true;
  options_.level_compaction_dynamic_level_bytes = true;
  options_.blob_file_discardable_ratio = 0.5;
  options_.purge_obsolete_files_period_sec = 0;
  NewDB();
  ColumnFamilyMetaData cf_meta;
  std::vector<std::string> to_compact;
  auto opts = db_->GetOptions();

  for (int i = 0; i < 10; i++) {
    db_->Put(WriteOptions(), GenKey(i), GenValue(i));
  }
  Flush();
  CheckBlobNumber(1);

  // compact level0 file to last level
  db_->GetColumnFamilyMetaData(base_db_->DefaultColumnFamily(), &cf_meta);
  to_compact.push_back(cf_meta.levels[0].files[0].name);
  db_->CompactFiles(CompactionOptions(), base_db_->DefaultColumnFamily(),
                    to_compact, opts.num_levels - 1);
  CheckBlobNumber(2);

  // update most of keys
  for (int i = 1; i < 11; i++) {
    db_->Put(WriteOptions(), GenKey(i), GenValue(i));
  }
  Flush();
  CheckBlobNumber(3);

  // compact new level0 file to last level
  db_->GetColumnFamilyMetaData(base_db_->DefaultColumnFamily(), &cf_meta);
  to_compact[0] = cf_meta.levels[0].files[0].name;
  db_->CompactFiles(CompactionOptions(), base_db_->DefaultColumnFamily(),
                    to_compact, opts.num_levels - 1);
  CheckBlobNumber(4);

  auto b = GetBlobStorage(base_db_->DefaultColumnFamily()->GetID()).lock();
  // blob file number starts from 2, they are: old level0 blob file, old
  // last level blob file, new level0 blob file, new last level blob file
  // respectively.
  ASSERT_EQ(b->FindFile(2).lock()->file_state(),
            BlobFileMeta::FileState::kObsolete);
  ASSERT_EQ(b->FindFile(3).lock()->file_state(),
            BlobFileMeta::FileState::kToMerge);
  ASSERT_EQ(b->FindFile(4).lock()->file_state(),
            BlobFileMeta::FileState::kObsolete);
  ASSERT_EQ(b->FindFile(5).lock()->file_state(),
            BlobFileMeta::FileState::kNormal);
}

TEST_P(BlobGCJobTest, RangeMergeScheduler) {
  NewDB();
  auto init_files =
      [&](std::vector<std::vector<std::pair<std::string, std::string>>>
              file_runs) {
        std::vector<std::shared_ptr<BlobFileMeta>> files;
        int file_num = 0;
        for (auto& run : file_runs) {
          for (auto& range : run) {
            auto file = std::make_shared<BlobFileMeta>(
                file_num++, 0, 0, 0, range.first, range.second);
            file->FileStateTransit(BlobFileMeta::FileEvent::kReset);
            files.emplace_back(file);
          }
        }
        return files;
      };

  // max_sorted_run = 1
  // run 1: [a, b] [c, d] [e, f] [g, h] [i, j] [k, l]
  // no file will be marked
  auto file_runs =
      std::vector<std::vector<std::pair<std::string, std::string>>>{
          {{"a", "b"},
           {"c", "d"},
           {"e", "f"},
           {"g", "h"},
           {"i", "j"},
           {"k", "l"}},
      };
  auto files = init_files(file_runs);
  ScheduleRangeMerge(files, 1);
  for (const auto& file : files) {
    ASSERT_EQ(file->file_state(), BlobFileMeta::FileState::kNormal);
  }

  // max_sorted_run = 1
  // run 1: [a, b] [c, d] [e, f] [g, h] [i, j] [k, l]
  // run 2:               [e, f] [g, h]
  // files overlapped with [e, h] will be marked
  file_runs = std::vector<std::vector<std::pair<std::string, std::string>>>{
      {{"a", "b"}, {"c", "d"}, {"e", "f"}, {"g", "h"}, {"i", "j"}, {"k", "l"}},
      {{"e", "f"}, {"g", "h"}},
  };
  files = init_files(file_runs);
  ScheduleRangeMerge(files, 1);
  for (size_t i = 0; i < files.size(); i++) {
    if (i == 2 || i == 3 || i == 6 || i == 7) {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kToMerge);
      files[i]->FileStateTransit(BlobFileMeta::FileEvent::kReset);
    } else {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kNormal);
    }
  }

  // max_sorted_run = 1
  // run 1: [a, b] [c, d] [e, f] [g, h] [i, j] [k, l]
  // run 2: [a, b]        [e, f] [g, h]            [l, m]
  // files overlapped with [a, b] and [e, h] will be marked
  file_runs = std::vector<std::vector<std::pair<std::string, std::string>>>{
      {{"a", "b"}, {"c", "d"}, {"e", "f"}, {"g", "h"}, {"i", "j"}, {"k", "l"}},
      {{"a", "b"}, {"e", "f"}, {"g", "h"}, {"l", "m"}},
  };
  files = init_files(file_runs);
  ScheduleRangeMerge(files, 1);
  for (size_t i = 0; i < files.size(); i++) {
    if (i == 1 || i == 4 || i == 5 || i == 9) {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kNormal);
    } else {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kToMerge);
      files[i]->FileStateTransit(BlobFileMeta::FileEvent::kReset);
    }
  }

  // max_sorted_run = 2
  // run 1: [a, b] [c, d] [e, f] [g, h] [i, j] [k, l]
  // run 2:        [c,                             l]
  // no file will be marked
  file_runs = std::vector<std::vector<std::pair<std::string, std::string>>>{
      {{"a", "b"}, {"c", "d"}, {"e", "f"}, {"g", "h"}, {"i", "j"}, {"k", "l"}},
      {{"c", "l"}},
  };
  files = init_files(file_runs);
  ScheduleRangeMerge(files, 2);
  for (const auto& file : files) {
    ASSERT_EQ(file->file_state(), BlobFileMeta::FileState::kNormal);
  }

  // max_sorted_run = 2
  // run 1: [a, b] [c, d] [e, f] [g, h] [i, j] [k, l]
  // run 2:        [c, d]
  // run 3:        [c, d]
  // files overlapped with [c, d] will be marked.
  file_runs = std::vector<std::vector<std::pair<std::string, std::string>>>{
      {{"a", "b"}, {"c", "d"}, {"e", "f"}, {"g", "h"}, {"i", "j"}, {"k", "l"}},
      {{"c", "d"}},
      {{"c", "d"}},
  };
  files = init_files(file_runs);
  ScheduleRangeMerge(files, 2);
  for (size_t i = 0; i < files.size(); i++) {
    if (i == 1 || i == 6 || i == 7) {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kToMerge);
      files[i]->FileStateTransit(BlobFileMeta::FileEvent::kReset);
    } else {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kNormal);
    }
  }

  // max_sorted_run = 2
  // run 1: [a, b] [c, d] [e, f] [g, h] [i, j] [k, l]
  // run 2:      [b1,  d]
  // run 3: [a,        d]
  // files overlapped with [c, d] will be marked.
  file_runs = std::vector<std::vector<std::pair<std::string, std::string>>>{
      {{"a", "b"}, {"c", "d"}, {"e", "f"}, {"g", "h"}, {"i", "j"}, {"k", "l"}},
      {{"b1", "d"}},
      {{"a", "d"}},
  };
  files = init_files(file_runs);
  ScheduleRangeMerge(files, 2);
  for (size_t i = 0; i < files.size(); i++) {
    if (i == 1 || i == 6 || i == 7) {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kToMerge);
      files[i]->FileStateTransit(BlobFileMeta::FileEvent::kReset);
    } else {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kNormal);
    }
  }

  // max_sorted_run = 2;
  // run 1: [a, b] [c, d] [e, f] [g, h] [i, j] [k, l]
  // run 2: [a, b]        [e, f] [g, h]            [l, m]
  // run 3:               [e,     g1]
  // files overlapped with [e, g] will be marked.
  file_runs = std::vector<std::vector<std::pair<std::string, std::string>>>{
      {{"a", "b"}, {"c", "d"}, {"e", "f"}, {"g", "h"}, {"i", "j"}, {"k", "l"}},
      {{"a", "b"}, {"e", "f"}, {"g", "h"}, {"l", "m"}},
      {{"e", "g1"}}};
  files = init_files(file_runs);
  ScheduleRangeMerge(files, 2);
  for (size_t i = 0; i < files.size(); i++) {
    if (i == 2 || i == 3 || i == 7 || i == 8 || i == 10) {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kToMerge);
      files[i]->FileStateTransit(BlobFileMeta::FileEvent::kReset);
    } else {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kNormal);
    }
  }

  // max_sorted_run = 2;
  // run 1: [a, b] [c, d] [e, f] [g, h] [i, j] [k, l]
  // run 2: [a, b]        [e, f] [g, h]            [l, m]
  // run 3: [a,                                      l1]
  // files overlapped with [a, b] and [e, h] will be marked.
  file_runs = std::vector<std::vector<std::pair<std::string, std::string>>>{
      {{"a", "b"}, {"c", "d"}, {"e", "f"}, {"g", "h"}, {"i", "j"}, {"k", "l"}},
      {{"a", "b"}, {"e", "f"}, {"g", "h"}, {"l", "m"}},
      {{"a", "l1"}}};
  files = init_files(file_runs);
  ScheduleRangeMerge(files, 2);
  for (size_t i = 0; i < files.size(); i++) {
    if (i == 1 || i == 4 || i == 5 || i == 9) {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kNormal);
    } else {
      ASSERT_EQ(files[i]->file_state(), BlobFileMeta::FileState::kToMerge);
      files[i]->FileStateTransit(BlobFileMeta::FileEvent::kReset);
    }
  }
}

TEST_P(BlobGCJobTest, RangeMerge) {
  options_.level_merge = true;
  options_.level_compaction_dynamic_level_bytes = true;
  options_.blob_file_discardable_ratio = 0.5;
  options_.range_merge = true;
  options_.max_sorted_runs = 4;
  options_.purge_obsolete_files_period_sec = 0;
  NewDB();
  ColumnFamilyMetaData cf_meta;
  std::vector<std::string> to_compact(1);
  auto opts = db_->GetOptions();

  // compact 5 sorted runs to last level of key range [1, 50]
  for (int i = 1; i <= 5; i++) {
    for (int j = 0; j < 10; j++) {
      db_->Put(WriteOptions(), GenKey(5 * j + i), GenValue(5 * j + i));
    }
    Flush();
    db_->GetColumnFamilyMetaData(base_db_->DefaultColumnFamily(), &cf_meta);
    to_compact[0] = cf_meta.levels[0].files[0].name;
    db_->CompactFiles(CompactionOptions(), base_db_->DefaultColumnFamily(),
                      to_compact, opts.num_levels - 1);
    CheckBlobNumber(2 * i);
  }

  auto b = GetBlobStorage(base_db_->DefaultColumnFamily()->GetID()).lock();
  // blob file number starts from 2. Even number blob files belong to level0,
  // odd number blob files belong to last level.
  for (int i = 2; i < 12; i++) {
    auto blob = b->FindFile(i).lock();
    if (i % 2 == 0) {
      ASSERT_EQ(blob->file_state(), BlobFileMeta::FileState::kObsolete);
    } else {
      ASSERT_EQ(blob->file_state(), BlobFileMeta::FileState::kToMerge);
    }
  }

  db_->GetColumnFamilyMetaData(base_db_->DefaultColumnFamily(), &cf_meta);
  to_compact[0] = cf_meta.levels[opts.num_levels - 1].files[0].name;
  db_->CompactFiles(CompactionOptions(), base_db_->DefaultColumnFamily(),
                    to_compact, opts.num_levels - 1);

  // after last level compaction, marked blob files are merged to new blob
  // files and obsoleted.
  for (int i = 2; i < 12; i++) {
    auto file = b->FindFile(i).lock();
    ASSERT_TRUE(file->NoLiveData());
    ASSERT_EQ(file->file_state(), BlobFileMeta::FileState::kObsolete);
  }
}

INSTANTIATE_TEST_CASE_P(BlobGCJobTestParameterized, BlobGCJobTest,
                        ::testing::Values(false, true));
}  // namespace titandb

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
