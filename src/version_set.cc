#include "version_set.h"

#include <inttypes.h>

#include "edit_collector.h"
#include "util/filename.h"

namespace rocksdb {
namespace titandb {

const size_t kMaxFileCacheSize = 1024 * 1024;

VersionSet::VersionSet(const TitanDBOptions& options, TitanStats* stats)
    : dirname_(options.dirname),
      env_(options.env),
      env_options_(options),
      db_options_(options),
      stats_(stats) {
  auto file_cache_size = db_options_.max_open_files;
  if (file_cache_size < 0) {
    file_cache_size = kMaxFileCacheSize;
  }
  file_cache_ = NewLRUCache(file_cache_size);
}

Status VersionSet::Open(
    const std::map<uint32_t, TitanCFOptions>& column_families) {
  // Sets up initial column families.
  AddColumnFamilies(column_families);

  Status s = env_->FileExists(CurrentFileName(dirname_));
  if (s.ok()) {
    return Recover();
  }
  if (!s.IsNotFound()) {
    return s;
  }
  return OpenManifest(NewFileNumber());
}

Status VersionSet::Recover() {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t, const Status& s) override {
      if (status->ok()) *status = s;
    }
  };

  // Reads "CURRENT" file, which contains the name of the current manifest file.
  std::string manifest;
  Status s = ReadFileToString(env_, CurrentFileName(dirname_), &manifest);
  if (!s.ok()) return s;
  if (manifest.empty() || manifest.back() != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  manifest.resize(manifest.size() - 1);

  // Opens the current manifest file.
  auto file_name = dirname_ + "/" + manifest;
  std::unique_ptr<SequentialFileReader> file;
  {
    std::unique_ptr<SequentialFile> f;
    s = env_->NewSequentialFile(file_name, &f,
                                env_->OptimizeForManifestRead(env_options_));
    if (!s.ok()) return s;
    file.reset(new SequentialFileReader(std::move(f), file_name));
  }

  // Reads edits from the manifest and applies them one by one.
  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(nullptr, std::move(file), &reporter, true /*checksum*/,
                       0 /*initial_offset*/, 0);
    Slice record;
    std::string scratch;
    EditCollector collector;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = DecodeInto(record, &edit);
      if (!s.ok()) return s;
      s = collector.AddEdit(edit);
      if (!s.ok()) return s;
    }
    s = collector.Apply(*this);
    if (!s.ok()) return s;
    uint64_t next_file_number = 0;
    s = collector.GetNextFileNumber(&next_file_number);
    if (!s.ok()) return s;
    next_file_number_.store(next_file_number);
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Next blob file number is %" PRIu64 ".", next_file_number);
  }

  auto new_manifest_file_number = NewFileNumber();
  s = OpenManifest(new_manifest_file_number);
  if (!s.ok()) return s;

  // Purge inactive files at start
  std::set<uint64_t> alive_files;
  alive_files.insert(new_manifest_file_number);
  for (const auto& bs : column_families_) {
    std::string files_str;
    for (const auto& f : bs.second->files_) {
      if (!files_str.empty()) {
        files_str.append(", ");
      }
      files_str.append(std::to_string(f.first));
      if (f.second->is_obsolete()) {
        files_str.append("(obsolete)");
      }
    }
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Blob files for CF %" PRIu32 " found: %s", bs.first,
                   files_str.c_str());
    // delete obsoleted files at reopen
    // all the obsolete files's obsolete sequence are 0
    bs.second->GetObsoleteFiles(nullptr, kMaxSequenceNumber);
    for (const auto& f : bs.second->files_) {
      alive_files.insert(f.second->file_number());
    }
  }
  std::vector<std::string> files;
  env_->GetChildren(dirname_, &files);
  for (const auto& f : files) {
    uint64_t file_number;
    FileType file_type;
    if (!ParseFileName(f, &file_number, &file_type)) continue;
    if (alive_files.find(file_number) != alive_files.end()) continue;
    if (file_type != FileType::kBlobFile &&
        file_type != FileType::kDescriptorFile)
      continue;
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Titan recovery delete obsolete file %s.", f.c_str());
    env_->DeleteFile(dirname_ + "/" + f);
  }

  // Make sure perform gc on all files at the beginning
  MarkAllFilesForGC();

  return Status::OK();
}

Status VersionSet::OpenManifest(uint64_t file_number) {
  Status s;

  auto file_name = DescriptorFileName(dirname_, file_number);
  std::unique_ptr<WritableFileWriter> file;
  {
    std::unique_ptr<WritableFile> f;
    s = env_->NewWritableFile(file_name, &f, env_options_);
    if (!s.ok()) return s;
    file.reset(new WritableFileWriter(std::move(f), file_name, env_options_));
  }

  manifest_.reset(new log::Writer(std::move(file), 0, false));

  // Saves current snapshot
  s = WriteSnapshot(manifest_.get());
  if (s.ok()) {
    ImmutableDBOptions ioptions(db_options_);
    s = SyncManifest(env_, &ioptions, manifest_->file());
  }
  if (s.ok()) {
    // Makes "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dirname_, file_number, nullptr);
  }

  if (!s.ok()) {
    manifest_.reset();
    obsolete_manifests_.emplace_back(file_name);
  }
  return s;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  Status s;
  // Saves global information
  {
    VersionEdit edit;
    edit.SetNextFileNumber(next_file_number_.load());
    std::string record;
    edit.EncodeTo(&record);
    s = log->AddRecord(record);
    if (!s.ok()) return s;
  }
  // Saves column families information
  for (auto& it : this->column_families_) {
    VersionEdit edit;
    edit.SetColumnFamilyID(it.first);
    for (auto& file : it.second->files_) {
      // skip obsolete file
      if (file.second->is_obsolete()) {
        continue;
      }
      edit.AddBlobFile(file.second);
    }
    std::string record;
    edit.EncodeTo(&record);
    s = log->AddRecord(record);
    if (!s.ok()) return s;
  }
  return s;
}

Status VersionSet::LogAndApply(VersionEdit& edit) {
  TEST_SYNC_POINT("VersionSet::LogAndApply");
  // TODO(@huachao): write manifest file unlocked
  std::string record;
  edit.SetNextFileNumber(next_file_number_.load());
  edit.EncodeTo(&record);
  Status s = manifest_->AddRecord(record);
  if (s.ok()) {
    ImmutableDBOptions ioptions(db_options_);
    s = SyncManifest(env_, &ioptions, manifest_->file());
  }
  if (s.ok()) {
    EditCollector collector;
    collector.AddEdit(edit);
    s = collector.Apply(*this);
  }
  return s;
}

void VersionSet::AddColumnFamilies(
    const std::map<uint32_t, TitanCFOptions>& column_families) {
  for (auto& cf : column_families) {
    auto file_cache = std::make_shared<BlobFileCache>(db_options_, cf.second,
                                                      file_cache_, stats_);
    auto blob_storage = std::make_shared<BlobStorage>(
        db_options_, cf.second, cf.first, file_cache, stats_);
    column_families_.emplace(cf.first, blob_storage);
  }
}

Status VersionSet::DropColumnFamilies(
    const std::vector<uint32_t>& column_families,
    SequenceNumber obsolete_sequence) {
  Status s;
  for (auto& cf_id : column_families) {
    auto it = column_families_.find(cf_id);
    if (it != column_families_.end()) {
      VersionEdit edit;
      edit.SetColumnFamilyID(it->first);
      for (auto& file : it->second->files_) {
        ROCKS_LOG_INFO(db_options_.info_log, "Titan add obsolete file [%llu]",
                       file.second->file_number());
        edit.DeleteBlobFile(file.first, obsolete_sequence);
      }
      s = LogAndApply(edit);
      if (!s.ok()) return s;
    } else {
      ROCKS_LOG_ERROR(db_options_.info_log, "column %u not found for drop\n",
                      cf_id);
      return Status::NotFound("invalid column family");
    }
    obsolete_columns_.insert(cf_id);
  }
  return s;
}

Status VersionSet::DestroyColumnFamily(uint32_t cf_id) {
  obsolete_columns_.erase(cf_id);
  auto it = column_families_.find(cf_id);
  if (it != column_families_.end()) {
    it->second->MarkDestroyed();
    if (it->second->MaybeRemove()) {
      column_families_.erase(it);
    }
    return Status::OK();
  }
  ROCKS_LOG_ERROR(db_options_.info_log, "column %u not found for destroy\n",
                  cf_id);
  return Status::NotFound("invalid column family");
}

void VersionSet::GetObsoleteFiles(std::vector<std::string>* obsolete_files,
                                  SequenceNumber oldest_sequence) {
  for (auto it = column_families_.begin(); it != column_families_.end();) {
    auto& cf_id = it->first;
    auto& blob_storage = it->second;
    // In the case of dropping column family, obsolete blob files can be deleted
    // only after the column family handle is destroyed.
    if (obsolete_columns_.find(cf_id) != obsolete_columns_.end()) {
      ++it;
      continue;
    }

    blob_storage->GetObsoleteFiles(obsolete_files, oldest_sequence);

    // Cleanup obsolete column family when all the blob files for that are
    // deleted.
    if (blob_storage->MaybeRemove()) {
      it = column_families_.erase(it);
      continue;
    }
    ++it;
  }

  obsolete_files->insert(obsolete_files->end(), obsolete_manifests_.begin(),
                         obsolete_manifests_.end());
  obsolete_manifests_.clear();
}

}  // namespace titandb
}  // namespace rocksdb
