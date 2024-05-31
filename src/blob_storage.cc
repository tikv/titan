#include "blob_storage.h"

#include "blob_file_set.h"
#include "titan_logging.h"

namespace rocksdb {
namespace titandb {

std::string BlobStorage::EncodeBlobCache(const BlobIndex& index) {
  std::string cache_key(cache_prefix_);
  PutVarint64(&cache_key, index.file_number);
  PutVarint64(&cache_key, index.blob_handle.offset);
  return cache_key;
}

Status BlobStorage::TryGetBlobCache(const std::string& cache_key,
                                    BlobRecord* record, PinnableSlice* value,
                                    bool* cache_hit) {
  Status s;
  Cache::Handle* cache_handle = blob_cache_->Lookup(cache_key);
  if (cache_handle) {
    *cache_hit = true;
    RecordTick(statistics(stats_), TITAN_BLOB_CACHE_HIT);
    auto blob = reinterpret_cast<OwnedSlice*>(blob_cache_->Value(cache_handle));
    s = DecodeInto(*blob, record);
    if (!s.ok()) return s;

    value->PinSlice(record->value, UnrefCacheHandle, blob_cache_.get(),
                    cache_handle);
    return s;
  }
  *cache_hit = false;
  RecordTick(statistics(stats_), TITAN_BLOB_CACHE_MISS);
  return s;
}

Status BlobStorage::Get(const ReadOptions& options, const BlobIndex& index,
                        BlobRecord* record, PinnableSlice* value) {
  std::string cache_key;

  if (blob_cache_) {
    cache_key = EncodeBlobCache(index);
    bool cache_hit;
    Status s = TryGetBlobCache(cache_key, record, value, &cache_hit);
    if (!s.ok()) return s;
    if (cache_hit) return s;
  }

  OwnedSlice blob;
  Status s = file_cache_->Get(options, index.file_number, index.blob_handle,
                              record, &blob);
  if (!s.ok()) {
    return s;
  }

  if (blob_cache_ && options.fill_cache) {
    Cache::Handle* cache_handle = nullptr;
    auto cache_value = new OwnedSlice(std::move(blob));
    blob_cache_->Insert(cache_key, cache_value, &kBlobValueCacheItemHelper,
                        cache_value->size() + sizeof(*cache_value),
                        &cache_handle, Cache::Priority::BOTTOM);
    value->PinSlice(record->value, UnrefCacheHandle, blob_cache_.get(),
                    cache_handle);
  } else {
    value->PinSlice(record->value, OwnedSlice::CleanupFunc, blob.release(),
                    nullptr);
  }
  return s;
}

Status BlobStorage::NewPrefetcher(uint64_t file_number,
                                  std::unique_ptr<BlobFilePrefetcher>* result) {
  return file_cache_->NewPrefetcher(file_number, result);
}

Status BlobStorage::GetBlobFilesInRanges(
    const RangePtr* ranges, size_t n, bool include_end,
    std::vector<std::shared_ptr<BlobFileMeta>>* files) {
  MutexLock l(&mutex_);
  for (size_t i = 0; i < n; i++) {
    const Slice* begin = ranges[i].start;
    const Slice* end = ranges[i].limit;
    auto cmp = cf_options_.comparator;

    std::string tmp;
    // nullptr means the minimum or maximum.
    for (auto it = ((begin != nullptr) ? blob_ranges_.lower_bound(*begin)
                                       : blob_ranges_.begin());
         it != ((end != nullptr) ? blob_ranges_.upper_bound(*end)
                                 : blob_ranges_.end());
         it++) {
      // The file is obsolete or being processed(such as GC), for safety just
      // skip.
      if (it->second->file_state() != BlobFileMeta::FileState::kNormal &&
          it->second->file_state() != BlobFileMeta::FileState::kToMerge) {
        continue;
      }

      // The smallest and largest key of blob file meta of the old version are
      // empty, so skip.
      if (it->second->largest_key().empty() && end) continue;

      if ((end == nullptr) ||
          (include_end && cmp->Compare(it->second->largest_key(), *end) <= 0) ||
          (!include_end && cmp->Compare(it->second->largest_key(), *end) < 0)) {
        files->push_back(it->second);
        if (!tmp.empty()) {
          tmp.append(" ");
        }
        tmp.append(std::to_string(it->second->file_number()));
      }
      assert(it->second->smallest_key().empty() ||
             (!begin || cmp->Compare(it->second->smallest_key(), *begin) >= 0));
    }
    TITAN_LOG_INFO(
        db_options_.info_log,
        "Get %" PRIuPTR " blob files [%s] in the range [%s, %s%c",
        files->size(), tmp.c_str(), begin ? begin->ToString(true).c_str() : " ",
        end ? end->ToString(true).c_str() : " ", include_end ? ']' : ')');
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
  ret.clear();
  MutexLock l(&mutex_);
  for (auto& kv : files_) {
    ret.emplace(kv.first, std::weak_ptr<BlobFileMeta>(kv.second));
  }
}

void BlobStorage::AddBlobFile(std::shared_ptr<BlobFileMeta>& file) {
  MutexLock l(&mutex_);
  files_.emplace(std::make_pair(file->file_number(), file));
  blob_ranges_.emplace(std::make_pair(Slice(file->smallest_key()), file));
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
  SubStats(stats_, cf_id_, TitanInternalStats::OBSOLETE_BLOB_FILE_SIZE,
           file->second->file_size());
  SubStats(stats_, cf_id_, TitanInternalStats::NUM_OBSOLETE_BLOB_FILE, 1);
  files_.erase(file_number);
  file_cache_->Evict(file_number);
  return true;
}

void BlobStorage::GetObsoleteFiles(std::vector<std::string>* obsolete_files,
                                   SequenceNumber oldest_sequence) {
  MutexLock l(&mutex_);

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
      TITAN_LOG_INFO(db_options_.info_log,
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

void BlobStorage::GetAllFiles(std::vector<std::string>* files) {
  MutexLock l(&mutex_);

  for (auto& file : files_) {
    uint64_t file_number = file.first;
    // relative to dirname
    files->emplace_back(BlobFileName("", file_number));
  }
}

void BlobStorage::UpdateStats() {
  MutexLock l(&mutex_);

  levels_file_count_.clear();
  levels_file_count_.assign(cf_options_.num_levels, 0);
  uint64_t live_blob_file_size = 0, num_live_blob_file = 0;
  uint64_t obsolete_blob_file_size = 0, num_obsolete_blob_file = 0;
  std::unordered_map<int, uint64_t> ratio_levels;

  // collect metrics
  for (auto& file : files_) {
    if (file.second->is_obsolete()) {
      num_obsolete_blob_file += 1;
      obsolete_blob_file_size += file.second->file_size();
      continue;
    }
    num_live_blob_file += 1;
    levels_file_count_[file.second->file_level()]++;

    // If the file is initialized yet, skip it
    if (file.second->file_state() != BlobFileMeta::FileState::kPendingInit) {
      live_blob_file_size += file.second->file_size();
      ratio_levels[static_cast<int>(file.second->GetDiscardableRatioLevel())] +=
          1;
    }
  }

  // update metrics
  SetStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_FILE_SIZE,
           live_blob_file_size);
  SetStats(stats_, cf_id_, TitanInternalStats::NUM_LIVE_BLOB_FILE,
           num_live_blob_file);
  SetStats(stats_, cf_id_, TitanInternalStats::OBSOLETE_BLOB_FILE_SIZE,
           obsolete_blob_file_size);
  SetStats(stats_, cf_id_, TitanInternalStats::NUM_OBSOLETE_BLOB_FILE,
           num_obsolete_blob_file);
  for (int i = TitanInternalStats::StatsType::NUM_DISCARDABLE_RATIO_LE0;
       i <= TitanInternalStats::StatsType::NUM_DISCARDABLE_RATIO_LE100; i++) {
    SetStats(stats_, cf_id_, static_cast<TitanInternalStats::StatsType>(i),
             ratio_levels[i]);
  }
}
void BlobStorage::ComputeGCScore() {
  UpdateStats();
  if (initialized_ && !initialized_->load(std::memory_order_acquire)) {
    return;
  }

  MutexLock l(&mutex_);
  gc_score_.clear();

  for (auto& file : files_) {
    if (file.second->is_obsolete()) {
      continue;
    }

    double score;
    if (file.second->file_size() < cf_options_.merge_small_file_threshold) {
      // for the small file or file with gc mark (usually the file that just
      // recovered) we want gc these file but more hope to gc other file with
      // more invalid data
      score = std::max(cf_options_.blob_file_discardable_ratio,
                       file.second->GetDiscardableRatio());
    } else {
      score = file.second->GetDiscardableRatio();
    }
    gc_score_.emplace_back(GCScore{
        .file_number = file.first,
        .score = score,
    });
  }

  std::sort(gc_score_.begin(), gc_score_.end(),
            [](const GCScore& first, const GCScore& second) {
              return first.score > second.score;
            });
}

}  // namespace titandb
}  // namespace rocksdb
