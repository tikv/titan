#include <thread>

#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "utilities/fault_injection_env.h"

#include "titan/checkpoint.h"
#include "titan/db.h"

namespace rocksdb {
namespace titandb {

class CheckpointTest : public testing::Test {
 protected:
  // Sequence of option configurations to try
  enum OptionConfig {
    kDefault = 0,
  };
  int option_config_;

 public:
  std::string dbname_;
  std::string alternative_wal_dir_;
  Env* env_;
  TitanDB* db_;
  TitanOptions last_options_;
  std::vector<ColumnFamilyHandle*> handles_;
  std::string snapshot_name_;

  CheckpointTest() : env_(Env::Default()) {
    env_->SetBackgroundThreads(1, Env::LOW);
    env_->SetBackgroundThreads(1, Env::HIGH);
    dbname_ = test::PerThreadDBPath(env_, "checkpoint_test");
    alternative_wal_dir_ = dbname_ + "/wal";
    auto options = CurrentOptions();
    auto delete_options = options;
    delete_options.wal_dir = alternative_wal_dir_;
    EXPECT_OK(DestroyTitanDB(dbname_, delete_options));
    // Destroy it for not alternative WAL dir is used.
    EXPECT_OK(DestroyTitanDB(dbname_, options));
    db_ = nullptr;
    snapshot_name_ = test::PerThreadDBPath(env_, "snapshot");
    std::string snapshot_tmp_name = snapshot_name_ + ".tmp";
    EXPECT_OK(DestroyTitanDB(snapshot_name_, options));
    env_->DeleteDir(snapshot_name_);
    EXPECT_OK(DestroyTitanDB(snapshot_tmp_name, options));
    env_->DeleteDir(snapshot_tmp_name);
    Reopen(options);
  }

  ~CheckpointTest() override {
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->LoadDependency({});
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

    Close();
    TitanOptions options;
    options.db_paths.emplace_back(dbname_, 0);
    options.db_paths.emplace_back(dbname_ + "_2", 0);
    options.db_paths.emplace_back(dbname_ + "_3", 0);
    options.db_paths.emplace_back(dbname_ + "_4", 0);
    EXPECT_OK(DestroyTitanDB(dbname_, options));
    EXPECT_OK(DestroyTitanDB(snapshot_name_, options));
  }

  // Return the current option configuration.
  TitanOptions CurrentOptions() {
    TitanOptions options;
    options.env = env_;
    options.create_if_missing = true;
    return options;
  }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            const TitanOptions& options) {
    ColumnFamilyOptions cf_opts(options);
    size_t cfi = handles_.size();
    handles_.resize(cfi + cfs.size());
    for (auto cf : cfs) {
      ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
    }
  }

  void DropColumnFamily(int cf) {
    auto handle = handles_[cf];
    ASSERT_OK(db_->DropColumnFamily(handle));
    ASSERT_OK(db_->DestroyColumnFamilyHandle(handle));
    handles_.erase(handles_.begin() + cf);
  }

  void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                             const TitanOptions& options) {
    CreateColumnFamilies(cfs, options);
    std::vector<std::string> cfs_plus_default = cfs;
    cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
    ReopenWithColumnFamilies(cfs_plus_default, options);
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const std::vector<TitanOptions>& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const TitanOptions& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const std::vector<TitanOptions>& options) {
    Close();
    EXPECT_EQ(cfs.size(), options.size());
    std::vector<TitanCFDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.push_back(TitanCFDescriptor(cfs[i], options[i]));
    }
    TitanDBOptions db_opts = TitanDBOptions(options[0]);
    return TitanDB::Open(db_opts, dbname_, column_families, &handles_, &db_);
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const TitanOptions& options) {
    Close();
    std::vector<TitanOptions> v_opts(cfs.size(), options);
    return TryReopenWithColumnFamilies(cfs, v_opts);
  }

  void Reopen(const TitanOptions& options) { ASSERT_OK(TryReopen(options)); }

  void CompactAll() {
    for (auto h : handles_) {
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), h, nullptr, nullptr));
    }
  }

  void Close() {
    for (auto h : handles_) {
      db_->DestroyColumnFamilyHandle(h);
    }
    handles_.clear();
    delete db_;
    db_ = nullptr;
  }

  void DestroyAndReopen(const TitanOptions& options) {
    // Destroy using last options
    Destroy(last_options_);
    ASSERT_OK(TryReopen(options));
  }

  void Destroy(const TitanOptions& options) {
    Close();
    ASSERT_OK(DestroyTitanDB(dbname_, options));
  }

  Status DestroyTitanDB(const std::string& dbname,
                        const TitanOptions& options) {
    // Clear and delete TitanDB directory first
    std::vector<std::string> filenames;
    std::string titandb_path;
    TitanOptions titan_options = options;
    if (titan_options.dirname.empty()) {
      titandb_path = dbname + "/titandb";
    } else {
      titandb_path = titan_options.dirname;
    }

    // Ignore error in case directory does not exist
    env_->GetChildren(titandb_path, &filenames);

    for (auto& fname : filenames) {
      std::string file_path = titandb_path + "/" + fname;
      env_->DeleteFile(file_path);
    }

    env_->DeleteDir(titandb_path);
    // Destroy base db
    return DestroyDB(dbname, titan_options.operator rocksdb::Options());
  }

  Status TryReopen(const TitanOptions& options) {
    Close();
    last_options_ = options;
    return TitanDB::Open(options, dbname_, &db_);
  }

  Status Flush(int cf = 0) {
    if (cf == 0) {
      return db_->Flush(FlushOptions());
    } else {
      return db_->Flush(FlushOptions(), handles_[cf]);
    }
  }

  std::string GenLargeValue(uint64_t min_blob_size, char v) {
    return std::string(min_blob_size + 1, v);
  }

  Status Put(const Slice& k, const Slice& v, WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, k, v);
  }

  Status Put(int cf, const Slice& k, const Slice& v,
             WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, handles_[cf], k, v);
  }

  Status Delete(const std::string& k) { return db_->Delete(WriteOptions(), k); }

  Status Delete(int cf, const std::string& k) {
    return db_->Delete(WriteOptions(), handles_[cf], k);
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  std::string Get(int cf, const std::string& k,
                  const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, handles_[cf], k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }
};

TEST_F(CheckpointTest, GetSnapshotLink) {
  for (uint64_t log_size_for_flush : {0, 1000000}) {
    TitanOptions options = CurrentOptions();
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyTitanDB(dbname_, options));

    // Create a database with small value and large value
    Status s;
    options.create_if_missing = true;
    ASSERT_OK(TitanDB::Open(options, dbname_, &db_));

    std::string small_key = std::string("small");
    std::string large_key = std::string("large");
    std::string small_value_v1 = std::string("v1");
    std::string small_value_v2 = std::string("v2");
    std::string large_value_v1 = GenLargeValue(options.min_blob_size, '1');
    std::string large_value_v2 = GenLargeValue(options.min_blob_size, '2');

    ASSERT_OK(Put(small_key, small_value_v1));
    ASSERT_EQ(small_value_v1, Get(small_key));
    ASSERT_OK(Put(large_key, large_value_v1));
    ASSERT_EQ(large_value_v1, Get(large_key));

    // Take a snapshot
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(
        checkpoint->CreateCheckpoint(snapshot_name_, "", log_size_for_flush));
    ASSERT_OK(Put(small_key, small_value_v2));
    ASSERT_EQ(small_value_v2, Get(small_key));
    ASSERT_OK(Put(large_key, large_value_v2));
    ASSERT_EQ(large_value_v2, Get(large_key));
    ASSERT_OK(Flush());
    ASSERT_EQ(small_value_v2, Get(small_key));
    ASSERT_EQ(large_value_v2, Get(large_key));
    // Open snapshot and verify contents while DB is running
    TitanDB* snapshotDB;
    ReadOptions roptions;
    std::string result;
    options.create_if_missing = false;
    ASSERT_OK(TitanDB::Open(options, snapshot_name_, &snapshotDB));
    ASSERT_OK(snapshotDB->Get(roptions, small_key, &result));
    ASSERT_EQ(small_value_v1, result);
    ASSERT_OK(snapshotDB->Get(roptions, large_key, &result));
    ASSERT_EQ(large_value_v1, result);
    delete snapshotDB;
    snapshotDB = nullptr;
    delete db_;
    db_ = nullptr;
    // Destroy original DB
    ASSERT_OK(DestroyTitanDB(dbname_, options));
    // Open snapshot and verify contents
    options.create_if_missing = false;
    dbname_ = snapshot_name_;
    ASSERT_OK(TitanDB::Open(TitanOptions(options), dbname_, &db_));
    ASSERT_EQ(small_value_v1, Get(small_key));
    ASSERT_EQ(large_value_v1, Get(large_key));
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyTitanDB(dbname_, options));
    delete checkpoint;
    // Restore DB name
    dbname_ = test::PerThreadDBPath(env_, "checkpoint_test");
  }
}

TEST_F(CheckpointTest, SpecifyTitanCheckpointDirectory) {
  TitanOptions options = CurrentOptions();
  delete db_;
  db_ = nullptr;
  ASSERT_OK(DestroyTitanDB(dbname_, options));

  // Create a database with small value and large value
  Status s;
  options.create_if_missing = true;
  ASSERT_OK(TitanDB::Open(options, dbname_, &db_));

  std::string small_key = std::string("small");
  std::string large_key = std::string("large");
  std::string small_value_v1 = std::string("v1");
  std::string small_value_v2 = std::string("v2");
  std::string large_value_v1 = GenLargeValue(options.min_blob_size, '1');
  std::string large_value_v2 = GenLargeValue(options.min_blob_size, '2');

  ASSERT_OK(Put(small_key, small_value_v1));
  ASSERT_EQ(small_value_v1, Get(small_key));
  ASSERT_OK(Put(large_key, large_value_v1));
  ASSERT_EQ(large_value_v1, Get(large_key));

  // Take a snapshot using a specific TitanDB directory
  Checkpoint* checkpoint;
  std::string titandb_snapshot_dir =
      test::PerThreadDBPath(env_, "snapshot-titandb");
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, titandb_snapshot_dir));
  ASSERT_OK(Put(small_key, small_value_v2));
  ASSERT_EQ(small_value_v2, Get(small_key));
  ASSERT_OK(Put(large_key, large_value_v2));
  ASSERT_EQ(large_value_v2, Get(large_key));
  ASSERT_OK(Flush());
  ASSERT_EQ(small_value_v2, Get(small_key));
  ASSERT_EQ(large_value_v2, Get(large_key));
  // Open snapshot and verify contents while DB is running
  TitanDB* snapshotDB;
  ReadOptions roptions;
  std::string result;
  options.create_if_missing = false;
  // Must specify the dirname
  options.dirname = titandb_snapshot_dir;
  ASSERT_OK(TitanDB::Open(options, snapshot_name_, &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, small_key, &result));
  ASSERT_EQ(small_value_v1, result);
  ASSERT_OK(snapshotDB->Get(roptions, large_key, &result));
  ASSERT_EQ(large_value_v1, result);
  delete snapshotDB;
  snapshotDB = nullptr;
  delete db_;
  db_ = nullptr;
  // Destroy original DB
  options.dirname = "";
  ASSERT_OK(DestroyTitanDB(dbname_, options));
  // Open snapshot and verify contents
  dbname_ = snapshot_name_;
  options.dirname = titandb_snapshot_dir;
  ASSERT_OK(TitanDB::Open(TitanOptions(options), dbname_, &db_));
  ASSERT_EQ(small_value_v1, Get(small_key));
  ASSERT_EQ(large_value_v1, Get(large_key));
  delete db_;
  db_ = nullptr;
  ASSERT_OK(DestroyTitanDB(dbname_, options));
  delete checkpoint;
  // Restore DB name
  dbname_ = test::PerThreadDBPath(env_, "checkpoint_test");
}

TEST_F(CheckpointTest, CheckpointCF) {
  TitanOptions options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two", "three", "four", "five", "six"},
                        options);
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"CheckpointTest::CheckpointCF:2", "DBImpl::FlushAllColumnFamilies:2"},
       {"DBImpl::FlushAllColumnFamilies:1", "CheckpointTest::CheckpointCF:1"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "one", "one"));
  ASSERT_OK(Put(2, "two", "two"));
  ASSERT_OK(Put(3, "three", "three"));
  ASSERT_OK(Put(4, "four", "four"));
  ASSERT_OK(Put(5, "five", "five"));
  std::string large_value_1 = GenLargeValue(options.min_blob_size, '1');
  ASSERT_OK(Put(6, "six", large_value_1));

  TitanDB* snapshotDB;
  ReadOptions roptions;
  std::string result;
  std::vector<ColumnFamilyHandle*> cphandles;
  Status s;

  // Take a snapshot
  rocksdb::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });
  TEST_SYNC_POINT("CheckpointTest::CheckpointCF:1");
  ASSERT_OK(Put(0, "Default", "Default1"));
  ASSERT_OK(Put(1, "one", "eleven"));
  ASSERT_OK(Put(2, "two", "twelve"));
  ASSERT_OK(Put(3, "three", "thirteen"));
  ASSERT_OK(Put(4, "four", "fourteen"));
  ASSERT_OK(Put(5, "five", "fifteen"));
  std::string large_value_2 = GenLargeValue(options.min_blob_size, '2');
  ASSERT_OK(Put(6, "six", large_value_2));
  TEST_SYNC_POINT("CheckpointTest::CheckpointCF:2");
  t.join();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(Put(1, "one", "twentyone"));
  ASSERT_OK(Put(2, "two", "twentytwo"));
  ASSERT_OK(Put(3, "three", "twentythree"));
  ASSERT_OK(Put(4, "four", "twentyfour"));
  ASSERT_OK(Put(5, "five", "twentyfive"));
  std::string large_value_3 = GenLargeValue(options.min_blob_size, '3');
  ASSERT_OK(Put(6, "six", large_value_3));
  ASSERT_OK(Flush());

  // Open snapshot and verify contents while DB is running
  options.create_if_missing = false;
  std::vector<std::string> cfs;
  cfs = {
      kDefaultColumnFamilyName, "one", "two", "three", "four", "five", "six"};
  std::vector<TitanCFDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.push_back(TitanCFDescriptor(cfs[i], options));
  }
  ASSERT_OK(TitanDB::Open(options, snapshot_name_, column_families, &cphandles,
                          &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "Default", &result));
  ASSERT_EQ("Default1", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "one", &result));
  ASSERT_EQ("eleven", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[6], "six", &result));
  ASSERT_EQ(large_value_2, result);
  for (auto h : cphandles) {
    snapshotDB->DestroyColumnFamilyHandle(h);
  }
  cphandles.clear();
  delete snapshotDB;
  snapshotDB = nullptr;
}

TEST_F(CheckpointTest, CheckpointCFNoFlush) {
  TitanOptions options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two", "three", "four", "five"}, options);

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "one", "one"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(2, "two", "two"));
  std::string large_value_1 = GenLargeValue(options.min_blob_size, '1');
  ASSERT_OK(Put(3, "three", large_value_1));

  TitanDB* snapshotDB;
  ReadOptions roptions;
  std::string result;
  std::vector<ColumnFamilyHandle*> cphandles;

  Status s;
  // Take a snapshot
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void* /*arg*/) {
        // Flush should never trigger.
        FAIL();
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, "", 1000000));
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  delete checkpoint;
  ASSERT_OK(Put(1, "one", "two"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(2, "two", "twentytwo"));
  std::string large_value_2 = GenLargeValue(options.min_blob_size, '2');
  ASSERT_OK(Put(3, "three", large_value_2));
  Close();
  EXPECT_OK(DestroyTitanDB(dbname_, options));

  // Open snapshot and verify contents while DB is running
  options.create_if_missing = false;
  std::vector<std::string> cfs;
  cfs = {kDefaultColumnFamilyName, "one", "two", "three", "four", "five"};
  std::vector<TitanCFDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.push_back(TitanCFDescriptor(cfs[i], options));
  }
  ASSERT_OK(TitanDB::Open(options, snapshot_name_, column_families, &cphandles,
                          &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "Default", &result));
  ASSERT_EQ("Default", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "one", &result));
  ASSERT_EQ("one", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "two", &result));
  ASSERT_EQ("two", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[3], "three", &result));
  ASSERT_EQ(large_value_1, result);
  for (auto h : cphandles) {
    snapshotDB->DestroyColumnFamilyHandle(h);
  }
  cphandles.clear();
  delete snapshotDB;
  snapshotDB = nullptr;
}

TEST_F(CheckpointTest, CurrentFileModifiedWhileCheckpointing) {
  TitanOptions options = CurrentOptions();
  options.max_manifest_file_size = 0;  // always rollover manifest for file add
  Reopen(options);

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"TitanCheckpointImpl::CreateCustomCheckpoint::AfterGetAllTitanFiles",
        "CheckpointTest::CurrentFileModifiedWhileCheckpointing:PrePut"},
       {"TitanCheckpointImpl::CreateCustomCheckpoint:BeforeTitanDBCheckpoint1",
        "VersionSet::LogAndApply:WriteManifest"},
       {"VersionSet::LogAndApply:WriteManifestDone",
        "TitanCheckpointImpl::CreateCustomCheckpoint::"
        "BeforeTitanDBCheckpoint2"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rocksdb::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });
  TEST_SYNC_POINT(
      "CheckpointTest::CurrentFileModifiedWhileCheckpointing:PrePut");
  ASSERT_OK(Put("Default", "Default1"));
  ASSERT_OK(Put("Large", GenLargeValue(options.min_blob_size, 'v')));
  ASSERT_OK(Flush());
  t.join();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  TitanDB* snapshotDB;
  // Successful Open() implies that CURRENT pointed to the manifest in the
  // checkpoint.
  ASSERT_OK(TitanDB::Open(options, snapshot_name_, &snapshotDB));
  delete snapshotDB;
  snapshotDB = nullptr;
}

TEST_F(CheckpointTest, CheckpointInvalidDirectoryName) {
  for (std::string checkpoint_dir : {"", "/", "////"}) {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_TRUE(
        checkpoint->CreateCheckpoint(checkpoint_dir).IsInvalidArgument());
    if (!checkpoint_dir.empty()) {
      ASSERT_TRUE(checkpoint->CreateCheckpoint(snapshot_name_, checkpoint_dir)
                      .IsInvalidArgument());
    }
    delete checkpoint;
  }
}

TEST_F(CheckpointTest, CheckpointWithParallelWrites) {
  ASSERT_OK(Put("key1", "val1"));
  port::Thread thread([this]() {
    ASSERT_OK(Put("key2", "val2"));
    ASSERT_OK(Put("key3", "val3"));
    ASSERT_OK(Put("key4", "val4"));
    ASSERT_OK(Put("key5", GenLargeValue(CurrentOptions().min_blob_size, '5')));
    ASSERT_OK(Put("key6", GenLargeValue(CurrentOptions().min_blob_size, '6')));
    ASSERT_OK(Put("key7", GenLargeValue(CurrentOptions().min_blob_size, '7')));
  });
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  thread.join();
}

TEST_F(CheckpointTest, CheckpointWithUnsyncedDataDropped) {
  TitanOptions options = CurrentOptions();
  std::unique_ptr<FaultInjectionTestEnv> env(new FaultInjectionTestEnv(env_));
  options.env = env.get();
  Reopen(options);
  ASSERT_OK(Put("key1", "val1"));
  std::string large_value = GenLargeValue(options.min_blob_size, 'v');
  ASSERT_OK(Put("key2", large_value));
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  env->DropUnsyncedFileData();

  // Make sure it's openable even though whatever data that wasn't synced got
  // dropped.
  options.env = env_;
  TitanDB* snapshot_db;
  ASSERT_OK(TitanDB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "key1", &get_result));
  ASSERT_EQ("val1", get_result);
  ASSERT_OK(snapshot_db->Get(read_opts, "key2", &get_result));
  ASSERT_EQ(large_value, get_result);
  delete snapshot_db;
  delete db_;
  db_ = nullptr;
}

TEST_F(CheckpointTest, GCWhileCheckpointing) {
  TitanOptions options = CurrentOptions();
  options.max_background_gc = 1;
  options.disable_background_gc = true;
  options.blob_file_discardable_ratio = 0.01;
  CreateAndReopenWithCF({"one", "two", "three"}, options);

  std::string large_value_1 = GenLargeValue(options.min_blob_size, '1');
  std::string large_value_2 = GenLargeValue(options.min_blob_size, '2');

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "one", "one"));
  ASSERT_OK(Put(2, "two", large_value_1));
  ASSERT_OK(Put(3, "three", large_value_2));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->LoadDependency(
      {{"TitanCheckpointImpl::CreateCustomCheckpoint::AfterGetAllTitanFiles",
        // Drop CF and GC after created base db checkpoint
        "CheckpointTest::DeleteBlobWhileCheckpointing::DropCF"},
       {"CheckpointTest::DeleteBlobWhileCheckpointing::WaitGC",
        "BlobGCJob::Finish::AfterRewriteValidKeyToLSM"},
       {"CheckpointTest::DeleteBlobWhileCheckpointing::GCFinish",
        "TitanCheckpointImpl::CreateCustomCheckpoint::"
        "BeforeTitanDBCheckpoint1"}});
  SyncPoint::GetInstance()->EnableProcessing();

  rocksdb::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });

  TEST_SYNC_POINT("CheckpointTest::DeleteBlobWhileCheckpointing::DropCF");
  DropColumnFamily(2);
  TEST_SYNC_POINT("CheckpointTest::DeleteBlobWhileCheckpointing::WaitGC");
  CompactAll();
  TEST_SYNC_POINT("CheckpointTest::DeleteBlobWhileCheckpointing::GCFinish");
  t.join();

  TitanDB* snapshotDB;
  ReadOptions roptions;
  std::string result;
  std::vector<ColumnFamilyHandle*> cphandles;
  options.create_if_missing = false;
  std::vector<std::string> cfs;
  cfs = {kDefaultColumnFamilyName, "one", "two", "three"};
  std::vector<TitanCFDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.push_back(TitanCFDescriptor(cfs[i], options));
  }
  ASSERT_OK(TitanDB::Open(options, snapshot_name_, column_families, &cphandles,
                          &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "Default", &result));
  ASSERT_EQ("Default", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "one", &result));
  ASSERT_EQ("one", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "two", &result));
  ASSERT_EQ(large_value_1, result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[3], "three", &result));
  ASSERT_EQ(large_value_2, result);
  CompactAll();
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "two", &result));
  ASSERT_EQ(large_value_1, result);
  for (auto h : cphandles) {
    snapshotDB->DestroyColumnFamilyHandle(h);
  }
  cphandles.clear();
  delete snapshotDB;
  snapshotDB = nullptr;
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
