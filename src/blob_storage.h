#pragma once
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>
#include "blob_file_cache.h"
#include "blob_format.h"
#include "blob_gc.h"
#include "rocksdb/options.h"
#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

// Provides methods to access the blob storage for a specific
// column family.
class BlobStorage {
 public:
  BlobStorage(const BlobStorage& bs) : destroyed_(false) {
    this->files_ = bs.files_;
    this->file_cache_ = bs.file_cache_;
    this->db_options_ = bs.db_options_;
    this->cf_options_ = bs.cf_options_;
    this->cf_id_ = bs.cf_id_;
    this->stats_ = bs.stats_;
  }

  BlobStorage(const TitanDBOptions& _db_options,
              const TitanCFOptions& _cf_options, uint32_t cf_id,
              std::shared_ptr<BlobFileCache> _file_cache, TitanStats* stats)
      : db_options_(_db_options),
        cf_options_(_cf_options),
        cf_id_(cf_id),
        levels_file_count_(_cf_options.num_levels, 0),
        blob_ranges_(InternalComparator(_cf_options.comparator)),
        file_cache_(_file_cache),
        destroyed_(false),
        stats_(stats) {}

  ~BlobStorage() {
    for (auto& file : files_) {
      file_cache_->Evict(file.second->file_number());
    }
  }

  const TitanDBOptions& db_options() { return db_options_; }

  const TitanCFOptions& cf_options() { return cf_options_; }

  const std::vector<GCScore> gc_score() {
    MutexLock l(&mutex_);
    return gc_score_;
  }

  // Gets the blob record pointed by the blob index. The provided
  // buffer is used to store the record data, so the buffer must be
  // valid when the record is used.
  Status Get(const ReadOptions& options, const BlobIndex& index,
             BlobRecord* record, PinnableSlice* buffer);

  // Creates a prefetcher for the specified file number.
  Status NewPrefetcher(uint64_t file_number,
                       std::unique_ptr<BlobFilePrefetcher>* result);

  // Get all the blob files within the ranges.
  Status GetBlobFilesInRanges(const RangePtr* ranges, size_t n,
                              bool include_end, std::vector<uint64_t>* files);

  // Finds the blob file meta for the specified file number. It is a
  // corruption if the file doesn't exist.
  std::weak_ptr<BlobFileMeta> FindFile(uint64_t file_number) const;

  // Must call before TitanDBImpl initialized.
  void InitializeAllFiles() {
    for (auto& file : files_) {
      file.second->FileStateTransit(BlobFileMeta::FileEvent::kDbRestart);
    }
    ComputeGCScore();
  }

  // The corresponding column family is dropped, so mark destroyed and we can
  // remove this blob storage later.
  void MarkDestroyed() {
    MutexLock l(&mutex_);
    destroyed_ = true;
  }

  // Returns whether this blob storage can be deleted now.
  bool MaybeRemove() const {
    MutexLock l(&mutex_);
    return destroyed_ && obsolete_files_.empty();
  }

  // Computes GC score.
  void ComputeGCScore();

  // Add a new blob file to this blob storage.
  void AddBlobFile(std::shared_ptr<BlobFileMeta>& file);

  // Gets all obsolete blob files whose obsolete_sequence is smaller than the
  // oldest_sequence. Note that the files returned would be erased from internal
  // structure, so for the next call, the files returned before wouldn't be
  // returned again.
  void GetObsoleteFiles(std::vector<std::string>* obsolete_files,
                        SequenceNumber oldest_sequence);

  // Mark the file as obsolete, and retrun value indicates whether the file is
  // founded.
  bool MarkFileObsolete(uint64_t file_number, SequenceNumber obsolete_sequence);

  // Returns the number of blob files, including obsolete files.
  std::size_t NumBlobFiles() const {
    MutexLock l(&mutex_);
    return files_.size();
  }

  int NumBlobFilesAtLevel(int level) const {
    MutexLock l(&mutex_);
    if (level >= static_cast<int>(levels_file_count_.size())) {
      return 0;
    }
    return levels_file_count_[level];
  }

  // Returns the number of obsolete blob files.
  // TODO: use this method to calculate `kNumObsoleteBlobFile` DB property.
  std::size_t NumObsoleteBlobFiles() const {
    MutexLock l(&mutex_);
    return obsolete_files_.size();
  }

  // Exports all blob files' meta. Only for tests.
  void ExportBlobFiles(
      std::map<uint64_t, std::weak_ptr<BlobFileMeta>>& ret) const;

 private:
  friend class BlobFileSet;
  friend class VersionTest;
  friend class BlobGCPickerTest;
  friend class BlobGCJobTest;
  friend class BlobFileSizeCollectorTest;

  void MarkFileObsoleteLocked(std::shared_ptr<BlobFileMeta> file,
                              SequenceNumber obsolete_sequence);
  bool RemoveFile(uint64_t file_number);

  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  uint32_t cf_id_;

  mutable port::Mutex mutex_;

  // Only BlobStorage OWNS BlobFileMeta
  // file_number -> file_meta
  std::unordered_map<uint64_t, std::shared_ptr<BlobFileMeta>> files_;
  std::vector<int> levels_file_count_;

  class InternalComparator {
   public:
    // The default constructor is not supposed to be used.
    // It is only to make std::multimap can compile.
    InternalComparator() : comparator_(nullptr){};
    explicit InternalComparator(const Comparator* comparator)
        : comparator_(comparator){};
    bool operator()(const Slice& key1, const Slice& key2) const {
      assert(comparator_ != nullptr);
      return comparator_->Compare(key1, key2) < 0;
    }

   private:
    const Comparator* comparator_;
  };
  // smallest_key -> file_meta
  std::multimap<const Slice, std::shared_ptr<BlobFileMeta>, InternalComparator>
      blob_ranges_;

  std::shared_ptr<BlobFileCache> file_cache_;

  std::vector<GCScore> gc_score_;

  std::list<std::pair<uint64_t, SequenceNumber>> obsolete_files_;
  // It is marked when the column family handle is destroyed, indicating the
  // in-memory data structure can be destroyed. Physical files may still be
  // kept.
  bool destroyed_;

  TitanStats* stats_;
};

}  // namespace titandb
}  // namespace rocksdb
