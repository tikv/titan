#include "blob_file_set.h"

#include <cinttypes>

#include "edit_collector.h"
#include "titan_logging.h"

namespace rocksdb {
namespace titandb {

const size_t kMaxFileCacheSize = 1024 * 1024;

BlobFileSet::BlobFileSet(const TitanDBOptions& options, TitanStats* stats,
                         std::atomic<bool>* initialized, port::Mutex* mutex)
    : dirname_(options.dirname),
      env_(options.env),
      env_options_(options),
      db_options_(options),
      stats_(stats),
      mutex_(mutex),
      initialized_(initialized) {
  auto file_cache_size = db_options_.max_open_files;
  if (file_cache_size < 0) {
    file_cache_size = kMaxFileCacheSize;
  }
  file_cache_ = NewLRUCache(file_cache_size);
}

Status BlobFileSet::Open(
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

Status BlobFileSet::Recover() {
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
    std::unique_ptr<FSSequentialFile> f;
    auto fs = env_->GetFileSystem();
    s = fs->NewSequentialFile(
        file_name, fs->OptimizeForManifestRead(FileOptions(env_options_)), &f,
        nullptr /*dbg*/);
    if (!s.ok()) return s;
    file.reset(new SequentialFileReader(std::move(f), file_name));
  }

  // Reads edits from the manifest and applies them one by one.
  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(nullptr, std::move(file), &reporter, true /*checksum*/,
                       0 /*log_num*/);
    Slice record;
    std::string scratch;
    EditCollector collector(db_options_.info_log.get(),
                            false);  // TODO: make paranoid check configurable
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = DecodeInto(record, &edit);
      if (!s.ok()) return s;
      s = collector.AddEdit(edit);
      if (!s.ok()) return s;
    }
    if (!s.ok()) return s;
    s = collector.Seal(*this);
    if (!s.ok()) return s;
    s = collector.Apply(*this);
    if (!s.ok()) return s;

    uint64_t next_file_number = 0;
    s = collector.GetNextFileNumber(&next_file_number);
    if (!s.ok()) return s;
    next_file_number_.store(next_file_number);
    TITAN_LOG_INFO(db_options_.info_log,
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
    TITAN_LOG_INFO(db_options_.info_log,
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
    TITAN_LOG_INFO(db_options_.info_log,
                   "Titan recovery delete obsolete file %s.", f.c_str());
    env_->DeleteFile(dirname_ + "/" + f);
  }

  return Status::OK();
}

Status BlobFileSet::OpenManifest(uint64_t file_number) {
  Status s;

  auto file_name = DescriptorFileName(dirname_, file_number);
  std::unique_ptr<WritableFileWriter> file;
  {
    std::unique_ptr<FSWritableFile> f;
    auto fs = env_->GetFileSystem();
    s = fs->NewWritableFile(file_name, FileOptions(env_options_), &f,
                            nullptr /*dbg*/);
    if (!s.ok()) return s;
    file.reset(new WritableFileWriter(std::move(f), file_name,
                                      FileOptions(env_options_)));
  }

  manifest_.reset(new log::Writer(std::move(file), 0, false));

  // Saves current snapshot
  s = WriteSnapshot(manifest_.get());
  if (s.ok()) {
    ImmutableDBOptions ioptions(db_options_);
    s = SyncTitanManifest(stats_, &ioptions, manifest_->file());
  }
  uint64_t old_manifest_file_number = manifest_file_number_;
  if (s.ok()) {
    // Makes "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_->GetFileSystem().get(), dirname_, file_number,
                       nullptr);
    manifest_file_number_ = file_number;
  }

  if (!s.ok()) {
    manifest_.reset();
    manifest_file_number_ = old_manifest_file_number;
    obsolete_manifests_.emplace_back(file_name);
  } else {
    opened_.store(true, std::memory_order_release);
  }
  return s;
}

Status BlobFileSet::WriteSnapshot(log::Writer* log) {
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

// A helper class to collect all the information needed for manifest write.
struct BlobFileSet::ManifestWriter {
  Status status;
  bool done;
  port::CondVar cv;
  EditCollector& collector;
  VersionEdit& edit;

  explicit ManifestWriter(port::Mutex* mu, EditCollector& _collector,
                          VersionEdit& _edit)
      : done(false), cv(mu), collector(_collector), edit(_edit) {}
};

Status BlobFileSet::LogAndApply(VersionEdit& edit) {
  mutex_->AssertHeld();
  TEST_SYNC_POINT("BlobFileSet::LogAndApply::Begin");
  edit.SetNextFileNumber(next_file_number_.load());

  EditCollector collector(db_options_.info_log.get(), true);
  Status s = collector.AddEdit(edit);
  if (!s.ok()) return s;
  s = collector.Seal(*this);
  if (!s.ok()) return s;

  ManifestWriter writer(mutex_, collector, edit);
  manifest_writers_.push_back(&writer);

  // The head of queue is the writer that is responsible for writing manifest
  // for the group. Thw write group makes sure only one thread is writing to the
  // manifest, otherwise the manifest will be corrupted.
  while (!writer.done && &writer != manifest_writers_.front()) {
    TEST_SYNC_POINT("BlobFileSet::LogAndApply::Wait");
    writer.cv.Wait();
  }
  if (writer.done) {
    // The writer is handled by the head of queue's writer, just return here
    return writer.status;
  }

  std::vector<VersionEdit*> batch_edits;
  std::vector<EditCollector*> collectors;
  ManifestWriter* last_writer = nullptr;
  auto it = manifest_writers_.cbegin();
  while (it != manifest_writers_.cend()) {
    last_writer = *(it++);
    assert(last_writer != nullptr);
    batch_edits.push_back(&last_writer->edit);
    collectors.push_back(&last_writer->collector);
  }

  {
    // Perform IO out of lock
    mutex_->Unlock();
    for (auto& e : batch_edits) {
      std::string record;
      e->EncodeTo(&record);
      s = manifest_->AddRecord(record);
      if (!s.ok()) {
        break;
      }
    }
    if (s.ok()) {
      ImmutableDBOptions ioptions(db_options_);
      s = SyncTitanManifest(stats_, &ioptions, manifest_->file());
      TEST_SYNC_POINT("BlobFileSet::LogAndApply::AfterSyncTitanManifest");
    }
    mutex_->Lock();
  }

  if (s.ok()) {
    for (auto& c : collectors) {
      // Apply the version edit to blob file set
      s = c->Apply(*this);
      if (!s.ok()) {
        break;
      }
    }
  }
  // Wake up all the waiting writers
  while (true) {
    ManifestWriter* ready = manifest_writers_.front();
    manifest_writers_.pop_front();
    ready->status = s;
    ready->done = true;
    if (ready != &writer) {
      ready->cv.Signal();
    }
    if (ready == last_writer) {
      break;
    }
  }
  if (!manifest_writers_.empty()) {
    manifest_writers_.front()->cv.Signal();
  }

  return s;
}

void BlobFileSet::AddColumnFamilies(
    const std::map<uint32_t, TitanCFOptions>& column_families) {
  for (auto& cf : column_families) {
    auto file_cache = std::make_shared<BlobFileCache>(db_options_, cf.second,
                                                      file_cache_, stats_);
    auto blob_storage = std::make_shared<BlobStorage>(
        db_options_, cf.second, cf.first, file_cache, stats_, initialized_);
    if (stats_ != nullptr) {
      stats_->InitializeCF(cf.first, blob_storage);
    }
    column_families_.emplace(cf.first, blob_storage);
  }
}

Status BlobFileSet::DropColumnFamilies(
    const std::vector<uint32_t>& column_families,
    SequenceNumber obsolete_sequence) {
  Status s;
  for (auto& cf_id : column_families) {
    auto it = column_families_.find(cf_id);
    if (it != column_families_.end()) {
      VersionEdit edit;
      edit.SetColumnFamilyID(it->first);
      for (auto& file : it->second->files_) {
        if (!file.second->is_obsolete()) {
          TITAN_LOG_INFO(db_options_.info_log,
                         "Titan add obsolete file [%" PRIu64 "]",
                         file.second->file_number());
          file.second->FileStateTransit(BlobFileMeta::FileEvent::kGCBegin);
          edit.DeleteBlobFile(file.first, obsolete_sequence);
        }
      }
      s = LogAndApply(edit);
      for (auto& file : it->second->files_) {
        file.second->FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);
      }
      if (!s.ok()) return s;
    } else {
      TITAN_LOG_ERROR(db_options_.info_log, "column %u not found for drop\n",
                      cf_id);
      return Status::NotFound("invalid column family");
    }
    obsolete_columns_.insert(cf_id);
  }
  return s;
}

Status BlobFileSet::MaybeDestroyColumnFamily(uint32_t cf_id) {
  obsolete_columns_.erase(cf_id);
  auto it = column_families_.find(cf_id);
  if (it != column_families_.end()) {
    it->second->MarkDestroyed();
    if (it->second->MaybeRemove()) {
      column_families_.erase(it);
    }
    return Status::OK();
  }
  TITAN_LOG_ERROR(db_options_.info_log, "column %u not found for destroy\n",
                  cf_id);
  return Status::NotFound("invalid column family");
}

Status BlobFileSet::DeleteBlobFilesInRanges(uint32_t cf_id,
                                            const RangePtr* ranges, size_t n,
                                            bool include_end,
                                            SequenceNumber obsolete_sequence) {
  mutex_->AssertHeld();
  TEST_SYNC_POINT("BlobFileSet::DeleteBlobFilesInRanges");
  auto it = column_families_.find(cf_id);
  if (it != column_families_.end()) {
    VersionEdit edit;
    edit.SetColumnFamilyID(cf_id);

    std::vector<std::shared_ptr<BlobFileMeta>> files;
    Status s = it->second->GetBlobFilesInRanges(ranges, n, include_end, &files);
    if (!s.ok()) return s;

    for (auto file : files) {
      // Mark files being deleted, so GC won't pick them up causing deleting the
      // blob file twice.
      file->FileStateTransit(BlobFileMeta::FileEvent::kGCBegin);
      TITAN_LOG_INFO(db_options_.info_log,
                     "Titan add obsolete file [%" PRIu64 "]",
                     file->file_number());
      edit.DeleteBlobFile(file->file_number(), obsolete_sequence);
    }
    s = LogAndApply(edit);
    for (auto file : files) {
      file->FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);
    }
    return s;
  }
  TITAN_LOG_ERROR(db_options_.info_log,
                  "column %u not found for delete blob files in ranges\n",
                  cf_id);
  return Status::NotFound("invalid column family");
}

void BlobFileSet::GetObsoleteFiles(std::vector<std::string>* obsolete_files,
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

void BlobFileSet::GetAllFiles(std::vector<std::string>* files,
                              std::vector<VersionEdit>* edits) {
  std::vector<std::string> all_blob_files;

  edits->clear();
  edits->reserve(column_families_.size());

  // Saves global information
  {
    VersionEdit edit;
    edit.SetNextFileNumber(next_file_number_.load());
    std::string record;
    edit.EncodeTo(&record);
    edits->emplace_back(edit);
  }

  // Saves all blob files
  for (auto& cf : column_families_) {
    VersionEdit edit;
    edit.SetColumnFamilyID(cf.first);
    auto& blob_storage = cf.second;
    blob_storage->GetAllFiles(&all_blob_files);
    for (auto& file : blob_storage->files_) {
      edit.AddBlobFile(file.second);
    }
    edits->emplace_back(edit);
  }

  files->clear();
  files->reserve(all_blob_files.size() + 2);

  for (auto& live_file : all_blob_files) {
    files->emplace_back(live_file);
  }

  // Append current MANIFEST and CURRENT file name
  files->emplace_back(DescriptorFileName("", manifest_file_number_));
  files->emplace_back(CurrentFileName(""));
}

}  // namespace titandb
}  // namespace rocksdb
