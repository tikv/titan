#include <inttypes.h>
#include <options/cf_options.h>
#include <initializer_list>
#include <map>
#include <vector>

#include "db_impl.h"
#include "db_iter.h"
#include "file/filename.h"
#include "port/port.h"
#include "rocksdb/utilities/debug.h"
#include "test_util/testharness.h"
#include "titan/db.h"
#include "util/random.h"

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

class TitanThreadSafetyTest : public testing::Test {
 public:
  TitanThreadSafetyTest() : dbname_(test::TmpDir()) {
    options_.dirname = dbname_ + "/titandb";
    options_.create_if_missing = true;
    options_.min_blob_size = 32;
    options_.min_gc_batch_size = 1;
    options_.blob_file_compression = CompressionType::kLZ4Compression;
    DeleteDir(env_, options_.dirname);
    DeleteDir(env_, dbname_);
  }

  ~TitanThreadSafetyTest() { Close(); }

  void Open() {
    ASSERT_OK(TitanDB::Open(options_, dbname_, &db_));
    db_impl_ = reinterpret_cast<TitanDBImpl*>(db_);
  }

  void Close() {
    if (!db_) return;
    ASSERT_OK(db_->Close());
    delete db_;
    db_ = nullptr;
  }

  void GC(ColumnFamilyHandle* handle) {
    ASSERT_OK(db_impl_->TEST_StartGC(handle->GetID()));
  }

  void PutMap(std::map<std::string, std::string>& data, uint64_t k) {
    std::string key = GenKey(k);
    std::string value = GenValue(k);
    data.emplace(key, value);
  }

  void PutCF(ColumnFamilyHandle* handle, uint64_t k) {
    WriteOptions wopts;
    std::string key = GenKey(k);
    std::string value = GenValue(k);
    ASSERT_OK(db_->Put(wopts, handle, key, value));
  }

  void DeleteCF(ColumnFamilyHandle* handle, uint64_t k) {
    WriteOptions wopts;
    std::string key = GenKey(k);
    ASSERT_OK(db_->Delete(wopts, handle, key));
  }

  void VerifyCF(ColumnFamilyHandle* handle,
                const std::map<std::string, std::string>& data,
                ReadOptions ropts = ReadOptions()) {
    db_impl_->PurgeObsoleteFiles();

    for (auto& kv : data) {
      std::string value;
      ASSERT_OK(db_->Get(ropts, handle, kv.first, &value));
      ASSERT_EQ(value, kv.second);
    }

    Iterator* iterator = db_->NewIterator(ropts, handle);
    iterator->SeekToFirst();
    for (auto& kv : data) {
      ASSERT_EQ(iterator->Valid(), true);
      ASSERT_EQ(iterator->key(), kv.first);
      ASSERT_EQ(iterator->value(), kv.second);
      iterator->Next();
    }
    delete iterator;
  }

  void ReadCF(ColumnFamilyHandle* handle,
              const std::map<std::string, std::string>& data,
              ReadOptions ropts = ReadOptions()) {
    db_impl_->PurgeObsoleteFiles();

    for (auto& kv : data) {
      std::string value;
      auto s = db_->Get(ropts, handle, kv.first, &value);
      ASSERT_TRUE(s.ok() || s.IsNotFound());
    }

    Iterator* iterator = db_->NewIterator(ropts, handle);
    iterator->SeekToFirst();
    while (iterator->Valid()) {
      iterator->Next();
    }
    delete iterator;
  }

  std::string GenKey(uint64_t k) {
    char buf[64];
    snprintf(buf, sizeof(buf), "k-%08" PRIu64, k);
    return buf;
  }

  std::string GenValue(uint64_t k) {
    if (k % 2 == 0) {
      return std::string(options_.min_blob_size - 1, 'v');
    } else {
      return std::string(options_.min_blob_size + 1, 'v');
    }
  }

  void RunJobs(
      std::initializer_list<std::function<void(ColumnFamilyHandle*)>> jobs) {
    Open();
    std::vector<port::Thread> threads;
    std::map<std::string, ColumnFamilyHandle*> handles;
    std::map<std::string, uint32_t> ref_count;
    uint32_t job_count = jobs.size();
    unfinished_worker_.store(job_count * param_.num_column_family,
                             std::memory_order_relaxed);
    for (uint32_t col = 0; col < param_.num_column_family; col++) {
      std::string name = std::to_string(col);
      TitanCFDescriptor desc(name, options_);
      ColumnFamilyHandle* handle = nullptr;
      ASSERT_OK(db_->CreateColumnFamily(desc, &handle));
      {
        MutexLock l(&mutex_);
        handles[name] = handle;
        ref_count[name] = job_count;
      }
      for (auto& job : jobs) {
        threads.emplace_back([&, handle, name] {
          for (uint32_t k = 0; k < param_.repeat; k++) {
            job(handle);
          }
          unfinished_worker_.fetch_sub(1, std::memory_order_relaxed);
          if (param_.sync) {
            while (unfinished_worker_.load(std::memory_order_relaxed) != 0) {
              job(handle);
            }
          }
          bool need_drop = false;
          {
            MutexLock l(&mutex_);
            if ((--ref_count[name]) == 0) {
              ref_count.erase(name);
              handles.erase(name);
              need_drop = true;
            }
          }
          if (need_drop) {
            ASSERT_OK(db_->DropColumnFamily(handle));
            db_->DestroyColumnFamilyHandle(handle);
          }
        });
      }
    }
    std::for_each(threads.begin(), threads.end(),
                  std::mem_fn(&port::Thread::join));
  }

  port::Mutex mutex_;
  Env* env_{Env::Default()};
  std::string dbname_;
  TitanOptions options_;
  TitanDB* db_{nullptr};
  TitanDBImpl* db_impl_{nullptr};

  struct TestParam {
    TestParam() = default;

    uint32_t num_column_family{4};

    uint32_t repeat{10};

    // when set true, faster worker will run extra jobs util slowest
    // worker finishes to maximize race condition.
    bool sync{true};
  } param_;
  std::atomic<uint32_t> unfinished_worker_;
};

TEST_F(TitanThreadSafetyTest, Insert) {
  const uint64_t kNumEntries = 100;
  std::map<std::string, std::string> data;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    PutMap(data, i);
  }
  ASSERT_EQ(kNumEntries, data.size());
  RunJobs({// Write and Flush and Verify
           [&](ColumnFamilyHandle* handle) {
             ASSERT_TRUE(handle != nullptr);
             for (uint64_t i = 1; i <= kNumEntries; i++) {
               PutCF(handle, i);
             }
             FlushOptions fopts;
             ASSERT_OK(db_->Flush(fopts, handle));
             VerifyCF(handle, data);
           },
           [&](ColumnFamilyHandle* handle) {
             ASSERT_TRUE(handle != nullptr);
             ReadCF(handle, data);
           },
           // Compact
           [&](ColumnFamilyHandle* handle) {
             ASSERT_TRUE(handle != nullptr);
             CompactRangeOptions copts;
             ASSERT_OK(db_->CompactRange(copts, handle, nullptr, nullptr));
           },
           // GC
           [&](ColumnFamilyHandle* handle) {
             ASSERT_TRUE(handle != nullptr);
             GC(handle);
           }});
}

TEST_F(TitanThreadSafetyTest, InsertAndDelete) {
  const uint64_t kNumEntries = 100;
  std::map<std::string, std::string> data;
  for (uint64_t i = 1; i <= kNumEntries; i++) {
    PutMap(data, i);
  }
  ASSERT_EQ(kNumEntries, data.size());
  RunJobs({// Insert and Flush
           [&](ColumnFamilyHandle* handle) {
             ASSERT_TRUE(handle != nullptr);
             for (uint64_t i = 1; i <= kNumEntries; i++) {
               PutCF(handle, i);
             }
             FlushOptions fopts;
             ASSERT_OK(db_->Flush(fopts, handle));
           },
           // Delete and Flush
           [&](ColumnFamilyHandle* handle) {
             ASSERT_TRUE(handle != nullptr);
             for (uint64_t i = 1; i <= kNumEntries; i++) {
               DeleteCF(handle, i);
             }
             FlushOptions fopts;
             ASSERT_OK(db_->Flush(fopts, handle));
           },
           // Read
           [&](ColumnFamilyHandle* handle) {
             ASSERT_TRUE(handle != nullptr);
             ReadCF(handle, data);
           },
           // Compact
           [&](ColumnFamilyHandle* handle) {
             ASSERT_TRUE(handle != nullptr);
             CompactRangeOptions copts;
             ASSERT_OK(db_->CompactRange(copts, handle, nullptr, nullptr));
           },
           // GC
           [&](ColumnFamilyHandle* handle) {
             ASSERT_TRUE(handle != nullptr);
             GC(handle);
           }});
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
