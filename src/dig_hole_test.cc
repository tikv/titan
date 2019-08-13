#include "blob_file_iterator.h"

#include <fcntl.h>
#include <cinttypes>
#include "blob_file_builder.h"
#include "blob_file_cache.h"
#include "blob_file_reader.h"
#include "dig_hole_job.h"
#include "env/io_posix.h"
#include "util/filename.h"
#include "util/testharness.h"
namespace rocksdb {
namespace titandb {
const uint64_t kRandomMax = 1000;
const uint64_t kTestStep = 10;
uint64_t Random() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_usec % kRandomMax;
}
class DigHoleTest : public testing::Test {
 public:
  Env *env_{Env::Default()};
  TitanOptions titan_options_;
  EnvOptions env_options_;
  std::string dirname_;
  std::string file_name_;
  uint64_t file_number_;
  TitanCFOptions titan_cf_options_;
  std::unique_ptr<BlobFileBuilder> builder_;
  std::unique_ptr<WritableFileWriter> writable_file_;
  std::unique_ptr<BlobFileIterator> blob_file_iterator_;
  std::unique_ptr<PosixRandomRWFile> random_rw_file_;
  std::shared_ptr<DigHoleJob> dig_hole_job_;
  std::unordered_map<std::string, BlobHandle *> data_;
  const uint64_t kKeyLength = 8;
  const uint64_t kBlobMaxFileSize = 256U << 20;
  const uint64_t kValueMaxLength = 32U << 10;
  const uint64_t kRecordNum = 1000;
  const uint64_t kBlockSize = 4096;
  uint64_t expect_before_size;
  uint64_t expect_after_size;

  DigHoleTest() : dirname_(test::TmpDir(env_)) {
    titan_options_.dirname = dirname_;
    file_number_ = Random::GetTLSInstance()->Next();
    file_name_ = BlobFileName(dirname_, file_number_);
    dig_hole_job_ = std::make_shared<DigHoleJob>(
        titan_options_, env_options_, env_, titan_cf_options_,
        std::bind(&DigHoleTest::IsShutingDown),
        std::bind(&DigHoleTest::DiscardEntry, this, std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3));
  }

  ~DigHoleTest() {
    env_->DeleteFile(file_name_);
    env_->DeleteDir(dirname_);
  }

  static bool IsShutingDown() { return false; }
  Status DiscardEntry(const Slice &key, const BlobIndex &blob_index,
                      bool *discardable) {
    std::string key_str = DecodeKey(key);
    *discardable = (data_.find(key_str) == data_.end());
    return Status::OK();
  }

  void GetRealSize(u_int64_t *size) {
    TitanDBOptions db_options(titan_options_);
    std::unique_ptr<PosixRandomRWFile>
        file;  // TODO(@DorianZheng) set read ahead size
    Status s =
        OpenBlobFile(file_number_, 0, db_options, env_options_, env_, &file);
    assert(s.ok());
    s = file->GetSizeOnDisk(size);
    assert(s.ok());
  }

  void GetExpectBeforeSize(const BlobHandle &the_last_handle) {
    expect_before_size =
        ((the_last_handle.offset + the_last_handle.size - 1) / kBlockSize + 1) *
            kBlockSize +
        kBlockSize /*foot*/;
    assert(expect_before_size % kBlockSize == 0);
  }

  void GetExpectAfterSize() {
    uint64_t blocks_num = expect_before_size / kBlockSize;
    std::vector<uint64_t> flags(blocks_num, 0);
    flags[0] = flags[blocks_num - 1] = 1;  // head and foot
    for (const auto &iter : data_) {
      uint64_t start = (iter.second->offset) / kBlockSize;
      uint64_t end = (iter.second->offset + iter.second->size) / kBlockSize;
      for (uint64_t i = start; i <= end; ++i) {
        flags[i] = 1;
      }
    }
    uint64_t count = 0;
    for (const auto &i : flags) {
      if (i) {
        count++;
      }
    }
    expect_after_size = count * kBlockSize;
  }

  std::string EncodeKey(const std::string &key) {
    int64_t remain =
        static_cast<int64_t>(kKeyLength) - static_cast<int64_t>(key.length());
    assert(remain >= 0);
    return std::string(remain, '0') + key;
  }

  std::string DecodeKey(const Slice &key) {
    std::string data = std::string(key.data());
    assert(data.length() >= kKeyLength);
    std::string ans = data.substr(0, kKeyLength);
    return ans;
  }

  void AddKeyValue(const std::string &key, const std::string &value,
                   BlobHandle *blob_handle) {
    std::string key_str = EncodeKey(key);
    BlobRecord record;
    record.key = key_str;
    record.value = value;
    builder_->Add(record, blob_handle);
    ASSERT_OK(builder_->status());
    data_.insert({key_str, blob_handle});
  }

  void DelKeyValue(const std::string &key) {
    std::string key_str = EncodeKey(key);
    data_.erase(key_str);
  }

  void NewBuilder() {
    TitanDBOptions db_options(titan_options_);
    TitanCFOptions cf_options(titan_options_);
    BlobFileCache cache(db_options, cf_options, {NewLRUCache(128)}, nullptr);
    {
      std::unique_ptr<WritableFile> f;
      ASSERT_OK(env_->NewWritableFile(file_name_, &f, env_options_));
      writable_file_.reset(
          new WritableFileWriter(std::move(f), file_name_, env_options_));
    }
    builder_.reset(
        new BlobFileBuilder(db_options, cf_options, writable_file_.get()));
    data_.clear();
  }

  void FinishBuilder() {
    ASSERT_OK(builder_->Finish());
    ASSERT_OK(builder_->status());
  }

  void CheckKeyExists() {
    // open file
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file_name_, &file_size));
    OpenBlobFile(file_number_, 0, titan_options_, env_options_, env_,
                 &random_rw_file_);
    blob_file_iterator_.reset(new BlobFileIterator{
        std::move(random_rw_file_), file_number_, file_size, TitanCFOptions()});
    // get keys in file
    std::unordered_set<std::string> keys_in_file;
    for (blob_file_iterator_->SeekToFirst(); blob_file_iterator_->Valid();
         blob_file_iterator_->Next()) {
      ASSERT_OK(blob_file_iterator_->status());
      std::string key_str = DecodeKey(blob_file_iterator_->key());
      keys_in_file.insert(key_str);
    }
    // data_ should be subset of keys_in_file
    for (auto &iter : data_) {
      bool ans = keys_in_file.find(iter.first) != keys_in_file.end();
      if (!ans) {
        // LOG(INFO) << iter.first;
        assert(ans);
      }
    }
  }
  // Write random number and random length records to blob file, delete random
  // number records and dig. Test size before dig and size after dig, and then
  // check remain key in the file.
  void Test(uint64_t threshold_discard) {
    NewBuilder();
    // add records
    const int n = Random() * kRecordNum / kRandomMax + 1;
    std::vector<BlobHandle> handles(n);
    for (int i = 0; i < n; i++) {
      auto id = std::to_string(i);
      std::string value =
          std::string(Random() * kValueMaxLength / kRandomMax, 'v');
      AddKeyValue(id, value, &handles[i]);
      assert(handles[i].offset < kBlobMaxFileSize);
    }
    FinishBuilder();
    GetExpectBeforeSize(handles[n - 1]);
    // del records
    for (int32_t i = 0; i < n; i++) {
      auto id = std::to_string(i);
      if (Random() > threshold_discard) {
        DelKeyValue(id);
      }
    }
    // check before size
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file_name_, &file_size));
    uint64_t before_size = 0;
    GetRealSize(&before_size);
    ASSERT_EQ(before_size, expect_before_size);
    // dig
    BlobFileMeta blob_file_meta(file_number_, file_size);
    blob_file_meta.set_real_file_size(file_size);
    blob_file_meta.FileStateTransit(BlobFileMeta::FileEvent::kDbRestart);
    blob_file_meta.FileStateTransit(BlobFileMeta::FileEvent::kGCBegin);
    Status s = dig_hole_job_->Exec(&blob_file_meta);
    assert(s.ok());
    // check after size
    uint64_t after_size = 0;
    GetRealSize(&after_size);
    assert(before_size >= after_size);
    GetExpectAfterSize();
#ifdef FALLOC_FL_PUNCH_HOLE
    ASSERT_EQ(after_size, expect_after_size);
#endif
    ASSERT_EQ(after_size, blob_file_meta.real_file_size());
    // check
    CheckKeyExists();
  }
};

TEST_F(DigHoleTest, Test) {
  assert(kRandomMax % kTestStep == 0);
  for (uint32_t i = 0; i <= kRandomMax; i += kTestStep) {
    Test(i);
  }
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
