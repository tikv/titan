#include "blob_file_iterator.h"

#include <cinttypes>

#include "blob_file_builder.h"
#include "blob_file_cache.h"
#include "blob_file_reader.h"
#include "dig_hole_job.h"
#include "env/io_posix.h"
#include "glog/logging.h"
#include "util/filename.h"
#include "util/testharness.h"
namespace rocksdb {
namespace titandb {

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

  DigHoleTest() : dirname_(test::TmpDir(env_)) {
    titan_options_.dirname = dirname_;
    file_number_ = Random::GetTLSInstance()->Next();
    file_name_ = BlobFileName(dirname_, file_number_);
    dig_hole_job_ = std::make_shared<DigHoleJob>(
        titan_options_, env_options_, env_, titan_cf_options_,
        std::bind(&DigHoleTest::IsShutingDown, this),
        std::bind(&DigHoleTest::DiscardEntry, this, std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3));
  }

  ~DigHoleTest() {
    env_->DeleteFile(file_name_);
    env_->DeleteDir(dirname_);
  }

  std::string GenKey(uint64_t i) {
    char buf[64];
    snprintf(buf, sizeof(buf), "k-%08" PRIu64, i);
    return buf;
  }

  std::string GenValue(uint64_t len) { return std::string(len, 'v'); }

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
  }

  void AddKeyValue(const std::string &key,
                   const std::string &value,  // uint64_t value_len,
                   BlobHandle *blob_handle) {
    BlobRecord record;
    int remain = kKeyLength - static_cast<int64_t>(key.length());
    assert(remain >= 0);
    record.key = std::string(remain, '0') + key;
    record.value = value;  // GenValue(value_len);
    builder_->Add(record, blob_handle);
    ASSERT_OK(builder_->status());
    data_.insert({record.key.data(), blob_handle});
  }

  void DelKeyValue(const std::string &key) { data_.erase(key); }

  void FinishBuilder() {
    ASSERT_OK(builder_->Finish());
    ASSERT_OK(builder_->status());
  }

  bool IsShutingDown() { return false; }
  Status DiscardEntry(const Slice &key, const BlobIndex &blob_index,
                      bool *discardable) {
    std::string data = std::string(key.data());
    std::string key_str = data.substr(0, kKeyLength);
    *discardable = (data_.find(key_str) == data_.end());
    return Status::OK();
  }

  void testDig(uint64_t expect_before_size, uint64_t expect_after_size) {
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file_name_, &file_size));

    uint64_t before_size = 0;
    GetRealSize(&before_size);
    ASSERT_EQ(before_size, expect_before_size);

    BlobFileMeta blob_file_meta(file_number_, file_size);
    dig_hole_job_->Exec(&blob_file_meta);

    uint64_t after_size = 0;
    GetRealSize(&after_size);
    ASSERT_EQ(after_size, expect_after_size);
    // LOG(INFO) << "before: " << before_size << " after: " << after_size;
  }

  uint64_t GetInitOffset(const BlobHandle &handle) {
    return ((handle.offset + handle.size - 1) / 4096 + 1) * 4096 +
           4096 /*foot*/;
  }

  void testTotalData(bool enable_del) {
    NewBuilder();

    Random random(19260817);//todo add real random
    const int n = random.Uniform(1000);
    std::vector<BlobHandle> handles(n);
    for (int i = 0; i < n; i++) {
      auto id = std::to_string(i);
      AddKeyValue(id, GenValue(i + 1), &handles[i]);
    }
    FinishBuilder();
    int64_t blob_size = GetInitOffset(handles[n - 1]);
    if(enable_del){
      data_.clear();
      testDig(blob_size, 8192);  // head+foot
    }else{
      testDig(blob_size, blob_size);
    }
  }
};

TEST_F(DigHoleTest, testDelNone) { testTotalData(true); }
TEST_F(DigHoleTest, testDelAll) { testTotalData(false); }

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
