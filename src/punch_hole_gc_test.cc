#include "test_util/testharness.h"

#include "db_impl.h"

namespace rocksdb {
namespace titandb {
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

class PunchHoleGCTest : public testing::Test {
 public:
  std::string dbname_;
  TitanDB* db_;
  DBImpl* base_db_;
  TitanDBImpl* tdb_;
  BlobFileSet* blob_file_set_;
  TitanOptions options_;
  port::Mutex* mutex_;

  PunchHoleGCTest() : dbname_(test::TmpDir()) {
    options_.dirname = dbname_ + "/titandb";
    options_.create_if_missing = true;
    options_.disable_background_gc = true;
    options_.min_blob_size = 0;
    options_.disable_auto_compactions = true;
    options_.env->CreateDirIfMissing(dbname_);
    options_.env->CreateDirIfMissing(options_.dirname);
  }
  ~PunchHoleGCTest() { Close(); }

  void DisableMergeSmall() { options_.merge_small_file_threshold = 0; }

  std::weak_ptr<BlobStorage> GetBlobStorage(uint32_t cf_id) {
    MutexLock l(mutex_);
    return blob_file_set_->GetBlobStorage(cf_id);
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

  void Close() {
    if (!db_) return;
    ASSERT_OK(db_->Close());
    delete db_;
    db_ = nullptr;
  }
};

TEST_F(PunchHoleGCTest, PunchHole) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"PunchHoleGCTest::PunchHole:AfterCompact",
        "TitanDBImpl::BackgroundCallGC:BeforeGCRunning"},
       {"TitanDBImpl::BackgroundCallGC:AfterGCRunning",
        "PunchHoleGCTest::PunchHole:BeforeCheckPunchHoleGCIsQueued"},
       //{"PunchHoleGCTest::PunchHole:AfterReleaseSnapshot",
       // "TitanDBImpl::MaybeRunPendingPunchHoleGC:"
       // "BeforeCheckPendingPunchHoleGC"},
       {"TitanDBImpl::MaybeRunPendingPunchHoleGC:AfterRunPendingPunchHoleGC",
        "PunchHoleGCTest::PunchHole:BeforeCheckPunchHoleGCIsFinished"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  DisableMergeSmall();
  options_.disable_background_gc = false;
  options_.disable_auto_compactions = false;
  options_.punch_hole_threshold = 4096;

  NewDB();
  auto b = GetBlobStorage(base_db_->DefaultColumnFamily()->GetID()).lock();
  std::vector<std::string> values(1000);
  for (int i = 0; i < 1000; i++) {
    values.push_back(GenValue(i));
    db_->Put(WriteOptions(), GenKey(i), values[i]);
  }
  Flush();
  std::map<uint64_t, std::weak_ptr<BlobFileMeta>> files;
  b->ExportBlobFiles(files);
  ASSERT_EQ(files.size(), 1);
  auto file_size = files.begin()->second.lock()->file_size();
  auto effective_file_size =
      files.begin()->second.lock()->effective_file_size();
  for (int i = 0; i < 1000; i++) {
    if (i % 3 == 0) {
      db_->Delete(WriteOptions(), GenKey(i));
    }
  }
  Flush();
  CompactAll();

  files.clear();
  b->ExportBlobFiles(files);
  ASSERT_EQ(files.size(), 1);
  ASSERT_EQ(files.begin()->second.lock()->GetHolePunchableSize(), 334 * 4096);
  ASSERT_EQ(files.begin()->second.lock()->effective_file_size(), 1000 * 4096);

  auto snapshot = db_->GetSnapshot();
  db_->Put(WriteOptions(), GenKey(100000), GenValue(1));

  TEST_SYNC_POINT("PunchHoleGCTest::PunchHole:AfterCompact");
  TEST_SYNC_POINT("PunchHoleGCTest::PunchHole:BeforeCheckPunchHoleGCIsQueued");

  files.clear();
  b->ExportBlobFiles(files);
  ASSERT_EQ(files.size(), 1);
  ASSERT_EQ(files.begin()->second.lock()->GetHolePunchableSize(), 334 * 4096);
  ASSERT_EQ(files.begin()->second.lock()->effective_file_size(), 1000 * 4096);

  db_->ReleaseSnapshot(snapshot);
  // TEST_SYNC_POINT("PunchHoleGCTest::PunchHole:AfterReleaseSnapshot");
  TEST_SYNC_POINT(
      "PunchHoleGCTest::PunchHole:BeforeCheckPunchHoleGCIsFinished");

  files.clear();
  b->ExportBlobFiles(files);
  ASSERT_EQ(files.size(), 1);
  auto post_punch_hole_file_size = files.begin()->second.lock()->file_size();
  ASSERT_EQ(post_punch_hole_file_size, file_size);
  ASSERT_EQ(files.begin()->second.lock()->effective_file_size(), 666 * 4096);
  ASSERT_EQ(files.begin()->second.lock()->GetHolePunchableSize(), 0);
  for (int i = 0; i < 1000; i++) {
    if (i % 3 == 0) {
      std::string value;
      db_->Get(ReadOptions(), GenKey(i), &value);
      ASSERT_EQ(value, values[i]);
    }
  }
  options_.disable_background_gc = true;
  options_.disable_auto_compactions = true;
  options_.punch_hole_threshold = 0;
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}