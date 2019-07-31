#include "blob_storage.h"
#include "version_set.h"

namespace rocksdb {
namespace titandb {

Status BlobStorage::Get(const ReadOptions& options, const BlobIndex& index,
                        BlobRecord* record, PinnableSlice* buffer) {
  auto sfile = FindFile(index.file_number).lock();
  if (!sfile)
    return Status::Corruption("Missing blob file: " +
                              std::to_string(index.file_number));
  return file_cache_->Get(options, sfile->file_number(), sfile->file_size(),
                          index.blob_handle, record, buffer);
}

Status BlobStorage::NewPrefetcher(uint64_t file_number,
                                  std::unique_ptr<BlobFilePrefetcher>* result) {
  auto sfile = FindFile(file_number).lock();
  if (!sfile)
    return Status::Corruption("Missing blob wfile: " +
                              std::to_string(file_number));
  return file_cache_->NewPrefetcher(sfile->file_number(), sfile->file_size(),
                                    result);
}

Status BlobStorage::DeleteBlobFilesInRanges(const RangePtr* ranges, size_t n,
                                            bool include_end,
                                            SequenceNumber obsolete_sequence) {
  MutexLock l(&mutex_);
  for (size_t i = 0; i < n; i++) {
    const Slice* begin = ranges[i].start;
    const Slice* end = ranges[i].limit;
    auto cmp = cf_options_.comparator;

    for (auto it = blob_ranges_.lower_bound(*begin);
         it != blob_ranges_.upper_bound(*end); it++) {
      // Obsolete files are to be deleted, so just skip.
      if (it->second->is_obsolete()) continue;
      // The smallest and largest key of blob file meta of the old version are
      // empty, so skip.
      if (it->second->largest_key().empty()) continue;

      if ((include_end && cmp->Compare(it->second->largest_key(), *end) <= 0) ||
          (!include_end && cmp->Compare(it->second->largest_key(), *end) < 0)) {
        MarkFileObsoleteLocked(it->second, obsolete_sequence);
      }
    }
  }
  return Status::OK();
}

std::weak_ptr<BlobFileMeta> BlobStorage::FindFile(uint64_t file_number) const {
  MutexLock l(&mutex_);
  auto it = files_.find(file_number);
  if (it != files_.end()) {
    assert(file_number == it->second->file_number());
    return it->second;
  }
  return std::weak_ptr<BlobFileMeta>();
}

void BlobStorage::ExportBlobFiles(
    std::map<uint64_t, std::weak_ptr<BlobFileMeta>>& ret) const {
  MutexLock l(&mutex_);
  for (auto& kv : files_) {
    ret.emplace(kv.first, std::weak_ptr<BlobFileMeta>(kv.second));
  }
}

void BlobStorage::AddBlobFile(std::shared_ptr<BlobFileMeta>& file) {
  MutexLock l(&mutex_);
  files_.emplace(std::make_pair(file->file_number(), file));
  blob_ranges_.emplace(std::make_pair(file->smallest_key(), file));
  AddStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_FILE_SIZE,
           file->file_size());
  AddStats(stats_, cf_id_, TitanInternalStats::NUM_LIVE_BLOB_FILE, 1);
}

bool BlobStorage::MarkFileObsolete(uint64_t file_number,
                                   SequenceNumber obsolete_sequence) {
  MutexLock l(&mutex_);
  auto file = files_.find(file_number);
  if (file == files_.end()) {
    return false;
  }
  MarkFileObsoleteLocked(file->second, obsolete_sequence);
  return true;
}

void BlobStorage::MarkFileObsoleteLocked(std::shared_ptr<BlobFileMeta> file,
                                         SequenceNumber obsolete_sequence) {
  mutex_.AssertHeld();

  obsolete_files_.push_back(
      std::make_pair(file->file_number(), obsolete_sequence));
  file->FileStateTransit(BlobFileMeta::FileEvent::kDelete);
  SubStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_SIZE,
           file->file_size() - file->discardable_size());
  SubStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_FILE_SIZE,
           file->file_size());
  SubStats(stats_, cf_id_, TitanInternalStats::NUM_LIVE_BLOB_FILE, 1);
  AddStats(stats_, cf_id_, TitanInternalStats::OBSOLETE_BLOB_FILE_SIZE,
           file->file_size());
  AddStats(stats_, cf_id_, TitanInternalStats::NUM_OBSOLETE_BLOB_FILE, 1);
}

bool BlobStorage::RemoveFile(uint64_t file_number) {
  mutex_.AssertHeld();

  auto file = files_.find(file_number);
  if (file == files_.end()) {
    return false;
  }
  // Removes from blob_ranges_
  auto p = blob_ranges_.equal_range(file->second->smallest_key());
  for (auto it = p.first; it != p.second; it++) {
    if (it->second->file_number() == file->second->file_number()) {
      it = blob_ranges_.erase(it);
      break;
    }
  }
  files_.erase(file_number);
  file_cache_->Evict(file_number);
  SubStats(stats_, cf_id_, TitanInternalStats::OBSOLETE_BLOB_FILE_SIZE,
           file->second->file_size());
  SubStats(stats_, cf_id_, TitanInternalStats::NUM_OBSOLETE_BLOB_FILE, 1);
  return true;
}

void BlobStorage::GetObsoleteFiles(std::vector<std::string>* obsolete_files,
                                   SequenceNumber oldest_sequence) {
  MutexLock l(&mutex_);

  uint32_t file_dropped = 0;
  uint64_t file_dropped_size = 0;
  for (auto it = obsolete_files_.begin(); it != obsolete_files_.end();) {
    auto& file_number = it->first;
    auto& obsolete_sequence = it->second;
    // We check whether the oldest snapshot is no less than the last sequence
    // by the time the blob file become obsolete. If so, the blob file is not
    // visible to all existing snapshots.
    if (oldest_sequence > obsolete_sequence) {
      // remove obsolete files
      bool __attribute__((__unused__)) removed = RemoveFile(file_number);
      assert(removed);
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Obsolete blob file %" PRIu64 " (obsolete at %" PRIu64
                     ") not visible to oldest snapshot %" PRIu64 ", delete it.",
                     file_number, obsolete_sequence, oldest_sequence);
      if (obsolete_files) {
        obsolete_files->emplace_back(
            BlobFileName(db_options_.dirname, file_number));
      }

      it = obsolete_files_.erase(it);
      continue;
    }
    ++it;
  }
}

void BlobStorage::ComputeGCScore() {
  // TODO: no need to recompute all everytime
  MutexLock l(&mutex_);
  gc_score_.clear();

  for (auto& file : files_) {
    if (file.second->is_obsolete()) {
      continue;
    }
    gc_score_.push_back({});
    auto& gcs = gc_score_.back();
    gcs.file_number = file.first;
    if (file.second->file_size() < cf_options_.merge_small_file_threshold ||
        file.second->gc_mark()) {
      // for the small file or file with gc mark (usually the file that just
      // recovered) we want gc these file but more hope to gc other file with
      // more invalid data
      gcs.score = cf_options_.blob_file_discardable_ratio;
    } else {
      gcs.score = file.second->GetDiscardableRatio();
    }
  }

  std::sort(gc_score_.begin(), gc_score_.end(),
            [](const GCScore& first, const GCScore& second) {
              return first.score > second.score;
            });
}

}  // namespace titandb
}  // namespace rocksdb
