#include "test_util/testharness.h"

#include "titan/db.h"

namespace rocksdb {
namespace titandb {

class TitanOptionsTest : public testing::Test {
 public:
  TitanOptionsTest() : db_name_(test::TmpDir()) {
    titan_options_.create_if_missing = true;
    titan_options_.dirname = db_name_ + "/titandb";
  }

  ~TitanOptionsTest() {
    Status s = Close();
    assert(s.ok());
  }

  Status Open() { return TitanDB::Open(titan_options_, db_name_, &titan_db); }

  Status DeleteDir(const std::string& dirname) {
    Status s;
    Env* env = Env::Default();
    std::vector<std::string> filenames;
    s = env->GetChildren(dirname, &filenames);
    if (!s.ok()) {
      return s;
    }
    for (auto& fname : filenames) {
      s = env->DeleteFile(dirname + "/" + fname);
      if (!s.ok()) {
        return s;
      }
    }
    s = env->DeleteDir(dirname);
    return s;
  }

  Status Close() {
    Status s;
    if (titan_db != nullptr) {
      s = titan_db->Close();
      if (!s.ok()) {
        return s;
      }
      titan_db = nullptr;
      s = DeleteDir(titan_options_.dirname);
      if (!s.ok()) {
        return s;
      }
      rocksdb::Options opts;
      s = rocksdb::DestroyDB(db_name_, opts);
    }
    return s;
  }

 protected:
  std::string db_name_;
  TitanOptions titan_options_;
  TitanDB* titan_db = nullptr;
};

TEST_F(TitanOptionsTest, LevelMerge) {
  titan_options_.level_merge = true;
  titan_options_.level_compaction_dynamic_level_bytes = false;
  Status s = Open();
  ASSERT_TRUE(s.IsInvalidArgument());
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
