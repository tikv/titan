#include "file/filename.h"
#include "test_util/testharness.h"

#include "blob_file_set.h"
#include "blob_format.h"
#include "edit_collector.h"
#include "testutil.h"
#include "util.h"
#include "version_edit.h"

namespace rocksdb {
namespace titandb {

void DeleteDir(Env* env, const std::string& dirname) {
  std::vector<std::string> filenames;
  env->GetChildren(dirname, &filenames);
  for (auto& fname : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(fname, &number, &type)) {
      ASSERT_OK(env->DeleteFile(dirname + "/" + fname));
    }
  }
  ASSERT_OK(env->DeleteDir(dirname));
}

class VersionTest : public testing::Test {
 public:
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  std::shared_ptr<BlobFileCache> file_cache_;
  std::map<uint32_t, std::shared_ptr<BlobStorage>> column_families_;
  std::unique_ptr<BlobFileSet> blob_file_set_;
  port::Mutex mutex_;
  std::string dbname_;
  Env* env_;

  VersionTest() : dbname_(test::TmpDir()), env_(Env::Default()) {
    db_options_.dirname = dbname_ + "/titandb";
    db_options_.create_if_missing = true;
    env_->CreateDirIfMissing(dbname_);
    env_->CreateDirIfMissing(db_options_.dirname);
    auto cache = NewLRUCache(db_options_.max_open_files);
    file_cache_.reset(
        new BlobFileCache(db_options_, cf_options_, cache, nullptr));
    Reset();
  }

  void Reset() {
    DeleteDir(env_, db_options_.dirname);
    env_->CreateDirIfMissing(db_options_.dirname);

    blob_file_set_.reset(new BlobFileSet(db_options_, nullptr));
    ASSERT_OK(blob_file_set_->Open({}));
    column_families_.clear();
    // Sets up some column families.
    for (uint32_t id = 0; id < 10; id++) {
      std::shared_ptr<BlobStorage> storage;
      storage.reset(
          new BlobStorage(db_options_, cf_options_, id, file_cache_, nullptr));
      column_families_.emplace(id, storage);
      storage.reset(
          new BlobStorage(db_options_, cf_options_, id, file_cache_, nullptr));
      blob_file_set_->column_families_.emplace(id, storage);
    }
  }

  void AddBlobFiles(uint32_t cf_id, uint64_t start, uint64_t end) {
    auto storage = column_families_[cf_id];
    for (auto i = start; i < end; i++) {
      auto file = std::make_shared<BlobFileMeta>(i, i, 0, 0, "", "");
      storage->files_.emplace(i, file);
    }
  }

  void DeleteBlobFiles(uint32_t cf_id, uint64_t start, uint64_t end) {
    auto& storage = column_families_[cf_id];
    for (auto i = start; i < end; i++) {
      storage->files_.erase(i);
    }
  }

  void BuildAndCheck(std::vector<VersionEdit> edits) {
    EditCollector collector;
    for (auto& edit : edits) {
      ASSERT_OK(collector.AddEdit(edit));
    }
    ASSERT_OK(collector.Seal(*blob_file_set_.get()));
    ASSERT_OK(collector.Apply(*blob_file_set_.get()));
    for (auto& it : blob_file_set_->column_families_) {
      auto& storage = column_families_[it.first];
      // ignore obsolete file
      auto size = 0;
      for (auto& file : it.second->files_) {
        if (!file.second->is_obsolete()) {
          size++;
        }
      }
      ASSERT_EQ(storage->files_.size(), size);
      for (auto& f : storage->files_) {
        auto iter = it.second->files_.find(f.first);
        ASSERT_TRUE(iter != it.second->files_.end());
        ASSERT_EQ(*f.second, *(iter->second));
      }
    }
  }

  void CheckColumnFamiliesSize(uint64_t size) {
    ASSERT_EQ(blob_file_set_->column_families_.size(), size);
  }

  void LegacyEncode(const VersionEdit& edit, std::string* dst) {
    PutVarint32Varint32(dst, Tag::kColumnFamilyID, edit.column_family_id_);

    for (auto& file : edit.added_files_) {
      PutVarint32(dst, Tag::kAddedBlobFile);
      PutVarint64(dst, file->file_number());
      PutVarint64(dst, file->file_size());
    }
    for (auto& file : edit.deleted_files_) {
      // obsolete sequence is a inpersistent field, so no need to encode it.
      PutVarint32Varint64(dst, Tag::kDeletedBlobFile, file.first);
    }
  }
};

TEST_F(VersionTest, VersionEdit) {
  VersionEdit input;
  CheckCodec(input);
  input.SetNextFileNumber(1);
  input.SetColumnFamilyID(2);
  CheckCodec(input);
  auto file1 = std::make_shared<BlobFileMeta>(3, 4, 0, 0, "", "");
  auto file2 = std::make_shared<BlobFileMeta>(5, 6, 0, 0, "", "");
  input.AddBlobFile(file1);
  input.AddBlobFile(file2);
  input.DeleteBlobFile(7, 0);
  input.DeleteBlobFile(8, 0);
  CheckCodec(input);
}

VersionEdit AddBlobFilesEdit(uint32_t cf_id, uint64_t start, uint64_t end) {
  VersionEdit edit;
  edit.SetColumnFamilyID(cf_id);
  for (auto i = start; i < end; i++) {
    auto file = std::make_shared<BlobFileMeta>(i, i, 0, 0, "", "");
    edit.AddBlobFile(file);
  }
  return edit;
}

VersionEdit DeleteBlobFilesEdit(uint32_t cf_id, uint64_t start, uint64_t end) {
  VersionEdit edit;
  edit.SetColumnFamilyID(cf_id);
  for (auto i = start; i < end; i++) {
    edit.DeleteBlobFile(i, 0);
  }
  return edit;
}

TEST_F(VersionTest, InvalidEdit) {
  // init state
  {
    auto add1_0_4 = AddBlobFilesEdit(1, 0, 4);
    EditCollector collector;
    ASSERT_OK(collector.AddEdit(add1_0_4));
    ASSERT_OK(collector.Seal(*blob_file_set_.get()));
    ASSERT_OK(collector.Apply(*blob_file_set_.get()));
  }

  // delete nonexistent blobs
  {
    auto del1_4_6 = DeleteBlobFilesEdit(1, 4, 6);
    EditCollector collector;
    ASSERT_OK(collector.AddEdit(del1_4_6));
    ASSERT_NOK(collector.Seal(*blob_file_set_.get()));
    ASSERT_NOK(collector.Apply(*blob_file_set_.get()));
  }

  // add already existing blobs
  {
    auto add1_1_3 = AddBlobFilesEdit(1, 1, 3);
    EditCollector collector;
    ASSERT_OK(collector.AddEdit(add1_1_3));
    ASSERT_NOK(collector.Seal(*blob_file_set_.get()));
    ASSERT_NOK(collector.Apply(*blob_file_set_.get()));
  }

  // add same blobs
  {
    auto add1_4_5_1 = AddBlobFilesEdit(1, 4, 5);
    auto add1_4_5_2 = AddBlobFilesEdit(1, 4, 5);
    EditCollector collector;
    ASSERT_OK(collector.AddEdit(add1_4_5_1));
    ASSERT_NOK(collector.AddEdit(add1_4_5_2));
    ASSERT_NOK(collector.Seal(*blob_file_set_.get()));
    ASSERT_NOK(collector.Apply(*blob_file_set_.get()));
  }

  // delete same blobs
  {
    auto del1_3_4_1 = DeleteBlobFilesEdit(1, 3, 4);
    auto del1_3_4_2 = DeleteBlobFilesEdit(1, 3, 4);
    EditCollector collector;
    ASSERT_OK(collector.AddEdit(del1_3_4_1));
    ASSERT_NOK(collector.AddEdit(del1_3_4_2));
    ASSERT_NOK(collector.Seal(*blob_file_set_.get()));
    ASSERT_NOK(collector.Apply(*blob_file_set_.get()));
  }
}

TEST_F(VersionTest, VersionBuilder) {
  // {(0, 4)}, {}
  auto add1_0_4 = AddBlobFilesEdit(1, 0, 4);
  AddBlobFiles(1, 0, 4);
  BuildAndCheck({add1_0_4});

  // {(0, 8)}, {(4, 8)}
  auto add1_4_8 = AddBlobFilesEdit(1, 4, 8);
  auto add2_4_8 = AddBlobFilesEdit(2, 4, 8);
  AddBlobFiles(1, 4, 8);
  AddBlobFiles(2, 4, 8);
  BuildAndCheck({add1_4_8, add2_4_8});

  // {(0, 4), (6, 8)}, {(4, 8)}
  auto del1_4_6 = DeleteBlobFilesEdit(1, 4, 6);
  DeleteBlobFiles(1, 4, 6);
  BuildAndCheck({del1_4_6});

  // {(0, 4)}, {(4, 6)}
  auto del1_6_8 = DeleteBlobFilesEdit(1, 6, 8);
  auto del2_6_8 = DeleteBlobFilesEdit(2, 6, 8);
  DeleteBlobFiles(1, 6, 8);
  DeleteBlobFiles(2, 6, 8);
  BuildAndCheck({del1_6_8, del2_6_8});

  // {(0, 4)}, {(4, 6)}
  Reset();
  AddBlobFiles(1, 0, 4);
  AddBlobFiles(2, 4, 6);
  add1_0_4 = AddBlobFilesEdit(1, 0, 4);
  add1_4_8 = AddBlobFilesEdit(1, 4, 8);
  add2_4_8 = AddBlobFilesEdit(2, 4, 8);
  del1_4_6 = DeleteBlobFilesEdit(1, 4, 6);
  del1_6_8 = DeleteBlobFilesEdit(1, 6, 8);
  del2_6_8 = DeleteBlobFilesEdit(2, 6, 8);
  BuildAndCheck({add1_0_4, add1_4_8, del1_4_6, del1_6_8, add2_4_8, del2_6_8});
}

TEST_F(VersionTest, ObsoleteFiles) {
  CheckColumnFamiliesSize(10);
  std::map<uint32_t, TitanCFOptions> m;
  m.insert({1, TitanCFOptions()});
  m.insert({2, TitanCFOptions()});
  blob_file_set_->AddColumnFamilies(m);
  {
    auto add1_1_5 = AddBlobFilesEdit(1, 1, 5);
    MutexLock l(&mutex_);
    blob_file_set_->LogAndApply(add1_1_5);
  }
  std::vector<std::string> of;
  blob_file_set_->GetObsoleteFiles(&of, kMaxSequenceNumber);
  ASSERT_EQ(of.size(), 0);
  {
    auto del1_4_5 = DeleteBlobFilesEdit(1, 4, 5);
    MutexLock l(&mutex_);
    blob_file_set_->LogAndApply(del1_4_5);
  }
  blob_file_set_->GetObsoleteFiles(&of, kMaxSequenceNumber);
  ASSERT_EQ(of.size(), 1);

  std::vector<uint32_t> cfs = {1};
  ASSERT_OK(blob_file_set_->DropColumnFamilies(cfs, 0));
  blob_file_set_->GetObsoleteFiles(&of, kMaxSequenceNumber);
  ASSERT_EQ(of.size(), 1);
  CheckColumnFamiliesSize(10);

  ASSERT_OK(blob_file_set_->MaybeDestroyColumnFamily(1));
  blob_file_set_->GetObsoleteFiles(&of, kMaxSequenceNumber);
  ASSERT_EQ(of.size(), 4);
  CheckColumnFamiliesSize(9);

  ASSERT_OK(blob_file_set_->MaybeDestroyColumnFamily(2));
  CheckColumnFamiliesSize(8);
}

TEST_F(VersionTest, DeleteBlobsInRange) {
  // The blob files' range are:
  // 1:[00--------------------------------------------------------99]
  // 2:[00----10]
  // 3:    [07---------------------55]
  // 4:                 [25------50]
  // 5:                 [25-------51]
  // 6:                         [50------65]
  // 7:                                                    [90----99]
  // 8:                         [50-------------79]
  // 9:                                    [70---80]
  // 10:                              [60----72]
  // 11:                                      [75-----------91]
  // 12:            [30--------------------------------85]
  // 13:                        [50--------------80]
  // 14:[]
  auto metas = std::vector<std::pair<std::string, std::string>>{
      std::make_pair("00", "99"), std::make_pair("00", "10"),
      std::make_pair("07", "55"), std::make_pair("25", "50"),
      std::make_pair("25", "51"), std::make_pair("50", "65"),
      std::make_pair("90", "99"), std::make_pair("50", "79"),
      std::make_pair("70", "80"), std::make_pair("60", "72"),
      std::make_pair("75", "91"), std::make_pair("30", "85"),
      std::make_pair("50", "80"), std::make_pair("", ""),
  };

  VersionEdit edit;
  edit.SetColumnFamilyID(1);
  for (size_t i = 0; i < metas.size(); i++) {
    auto file = std::make_shared<BlobFileMeta>(i + 1, i + 1, 0, 0,
                                               std::move(metas[i].first),
                                               std::move(metas[i].second));
    edit.AddBlobFile(file);
  }
  EditCollector collector;
  ASSERT_OK(collector.AddEdit(edit));
  ASSERT_OK(collector.Seal(*blob_file_set_.get()));
  ASSERT_OK(collector.Apply(*blob_file_set_.get()));

  Slice begin = Slice("50");
  Slice end = Slice("80");
  RangePtr range(&begin, &end);
  auto blob = blob_file_set_->GetBlobStorage(1).lock();

  blob_file_set_->DeleteBlobFilesInRanges(1, &range, 1, false /* include_end */,
                                          0);
  ASSERT_EQ(blob->NumBlobFiles(), metas.size());
  // obsolete files: 6, 8, 10
  ASSERT_EQ(blob->NumObsoleteBlobFiles(), 3);

  blob_file_set_->DeleteBlobFilesInRanges(1, &range, 1, true /* include_end */,
                                          0);
  ASSERT_EQ(blob->NumBlobFiles(), metas.size());
  // obsolete file: 6, 8, 9, 10, 13
  ASSERT_EQ(blob->NumObsoleteBlobFiles(), 5);

  std::vector<std::string> obsolete_files;
  blob_file_set_->GetObsoleteFiles(&obsolete_files, 1);
  ASSERT_EQ(blob->NumBlobFiles(), 9);

  Slice begin1 = Slice("");
  Slice end1 = Slice("99");
  RangePtr range1(&begin1, &end1);

  blob_file_set_->DeleteBlobFilesInRanges(1, &range1, 1,
                                          false /* include_end */, 0);
  // obsolete file: 2, 3, 4, 5, 11, 12
  ASSERT_EQ(blob->NumObsoleteBlobFiles(), 6);

  RangePtr range2(nullptr, nullptr);
  blob_file_set_->DeleteBlobFilesInRanges(1, &range2, 1, true /* include_end */,
                                          0);
  // obsolete file: 1, 2, 3, 4, 5, 7, 11, 12, 14
  ASSERT_EQ(blob->NumObsoleteBlobFiles(), 9);

  blob_file_set_->GetObsoleteFiles(&obsolete_files, 1);
  ASSERT_EQ(blob->NumBlobFiles(), 0);
}

TEST_F(VersionTest, BlobFileMetaV1ToV2) {
  VersionEdit edit;
  edit.SetColumnFamilyID(1);
  edit.AddBlobFile(std::make_shared<BlobFileMeta>(1, 1, 0, 0, "", ""));
  edit.DeleteBlobFile(1, 0);
  edit.AddBlobFile(std::make_shared<BlobFileMeta>(2, 2, 0, 0, "", ""));
  std::string str;
  LegacyEncode(edit, &str);

  VersionEdit edit1;
  ASSERT_OK(DecodeInto(Slice(str), &edit1));
  CheckCodec(edit1);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
