#include "db_impl.h"
#include "test_util/testharness.h"

namespace rocksdb {
namespace titandb {

class TestCompactionFilter : public CompactionFilter {
public:
  explicit TestCompactionFilter(uint64_t min_blob_size)
      : min_blob_size_(min_blob_size) {}

  const char *Name() const override { return "DeleteCompactionFilter"; }

  bool Filter(int level, const Slice &key, const Slice &value,
              std::string * /*&new_value*/,
              bool * /*value_changed*/) const override {
    AssertValue(key, value);
    return true;
  }

private:
  void AssertValue(const Slice &key, const Slice &value) const {
    if (key.ToString() == "mykey") {
      ASSERT_EQ(value.ToString(), "myvalue");
    }
    if (key.ToString() == "bigkey") {
      ASSERT_EQ(value.ToString(), std::string(min_blob_size_ + 1, 'v'));
    }
  }

  uint64_t min_blob_size_;
};

class TitanCompactionFilterTest : public testing::Test {
public:
  TitanCompactionFilterTest() : dbname_(test::TmpDir()) {
    options_.dirname = dbname_ + "/titandb";
    options_.create_if_missing = true;
    options_.disable_background_gc = true;
    options_.disable_auto_compactions = true;
    options_.compaction_filter =
        new TestCompactionFilter(options_.min_blob_size);

    DeleteDir(options_.dirname);
    DeleteDir(dbname_);
  }

  ~TitanCompactionFilterTest() override {
    Close();
    delete options_.compaction_filter;
    DeleteDir(options_.dirname);
    DeleteDir(dbname_);
  }

  static void DeleteDir(const std::string &dirname) {
    Env *env = Env::Default();
    std::vector<std::string> filenames;
    env->GetChildren(dirname, &filenames);

    for (auto &fname : filenames) {
      env->DeleteFile(dirname + "/" + fname);
    }
    env->DeleteDir(dirname);
  }

  Status Open() { return TitanDB::Open(options_, dbname_, &db_); }

  void Close() {
    if (!db_)
      return;

    ASSERT_OK(db_->Close());
    db_ = nullptr;
    rocksdb::Options opts;
    ASSERT_OK(rocksdb::DestroyDB(dbname_, opts));
  }

  Status Get(const std::string &key, std::string *value) {
    ReadOptions ropts;
    return db_->Get(ropts, key, value);
  }

  Status Put(const std::string &key, const std::string &value) {
    WriteOptions wopts;
    return db_->Put(wopts, key, value);
  }

  std::string GetBigValue() {
    return std::string(options_.min_blob_size + 1, 'v');
  }

  void CompactAll() {
    CompactRangeOptions copts;
    ASSERT_OK(db_->CompactRange(copts, nullptr, nullptr));
  }

protected:
  std::string dbname_;
  TitanOptions options_;
  TitanDB *db_{nullptr};
};

TEST_F(TitanCompactionFilterTest, CompactNormalValue) {
  Status s = Open();
  ASSERT_OK(s);

  s = Put("mykey", "myvalue");
  ASSERT_OK(s);

  std::string value;
  s = Get("mykey", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "myvalue");

  CompactAll();

  s = Get("mykey", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(TitanCompactionFilterTest, CompactBlobValue) {
  Status s = Open();
  ASSERT_OK(s);

  std::string value = GetBigValue();
  ASSERT_GT(value.length(), options_.min_blob_size);
  s = Put("bigkey", value);
  ASSERT_OK(s);

  std::string value1;
  s = Get("bigkey", &value1);
  ASSERT_OK(s);
  ASSERT_EQ(value1, value);

  CompactAll();

  s = Get("bigkey", &value1);
  ASSERT_TRUE(s.IsNotFound());
}

} // namespace titandb
} // namespace rocksdb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}