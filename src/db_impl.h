#pragma once

#include "db/db_impl/db_impl.h"
#include "rocksdb/statistics.h"
#include "rocksdb/threadpool.h"
#include "util/repeatable_thread.h"

#include "blob_file_manager.h"
#include "blob_file_set.h"
#include "blob_index_merge_operator.h"
#include "table_factory.h"
#include "titan/db.h"
#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

struct TitanColumnFamilyInfo {
  const std::string name;
  const ImmutableTitanCFOptions immutable_cf_options;
  MutableTitanCFOptions mutable_cf_options;
  std::shared_ptr<TableFactory> base_table_factory;
  std::shared_ptr<TitanTableFactory> titan_table_factory;
};

class TitanCompactionFilterFactory;
class TitanCompactionFilter;

class TitanDBImpl : public TitanDB {
 public:
  TitanDBImpl(const TitanDBOptions& options, const std::string& dbname);

  ~TitanDBImpl();

  Status Open(const std::vector<TitanCFDescriptor>& descs,
              std::vector<ColumnFamilyHandle*>* handles);

  Status Close() override;

  using TitanDB::CreateColumnFamilies;
  Status CreateColumnFamilies(
      const std::vector<TitanCFDescriptor>& descs,
      std::vector<ColumnFamilyHandle*>* handles) override;

  Status DropColumnFamilies(
      const std::vector<ColumnFamilyHandle*>& handles) override;

  Status DestroyColumnFamilyHandle(ColumnFamilyHandle* column_family) override;

  using TitanDB::CompactFiles;
  Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override;

  Status CloseImpl();

  using TitanDB::Put;
  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& value) override;

  using TitanDB::Write;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;

  using TitanDB::MultiBatchWrite;
  Status MultiBatchWrite(const WriteOptions& options,
                         std::vector<WriteBatch*>&& updates) override;

  using TitanDB::Delete;
  Status Delete(const WriteOptions& options, ColumnFamilyHandle* column_family,
                const Slice& key) override;

  using TitanDB::IngestExternalFile;
  Status IngestExternalFile(ColumnFamilyHandle* column_family,
                            const std::vector<std::string>& external_files,
                            const IngestExternalFileOptions& options) override;

  using TitanDB::CompactRange;
  Status CompactRange(const CompactRangeOptions& options,
                      ColumnFamilyHandle* column_family, const Slice* begin,
                      const Slice* end) override;

  using TitanDB::Flush;
  Status Flush(const FlushOptions& fopts,
               ColumnFamilyHandle* column_family) override;

  using TitanDB::Get;
  Status Get(const ReadOptions& options, ColumnFamilyHandle* handle,
             const Slice& key, PinnableSlice* value) override;

  using TitanDB::MultiGet;
  std::vector<Status> MultiGet(const ReadOptions& options,
                               const std::vector<ColumnFamilyHandle*>& handles,
                               const std::vector<Slice>& keys,
                               std::vector<std::string>* values) override;

  using TitanDB::NewIterator;
  Iterator* NewIterator(const TitanReadOptions& options,
                        ColumnFamilyHandle* handle) override;

  using TitanDB::NewIterators;
  Status NewIterators(const TitanReadOptions& options,
                      const std::vector<ColumnFamilyHandle*>& handles,
                      std::vector<Iterator*>* iterators) override;

  const Snapshot* GetSnapshot() override;

  void ReleaseSnapshot(const Snapshot* snapshot) override;

  using TitanDB::DisableFileDeletions;
  Status DisableFileDeletions() override;

  using TitanDB::EnableFileDeletions;
  Status EnableFileDeletions(bool force) override;

  using TitanDB::GetAllTitanFiles;
  Status GetAllTitanFiles(std::vector<std::string>& files,
                          std::vector<VersionEdit>* edits) override;

  Status DeleteFilesInRanges(ColumnFamilyHandle* column_family,
                             const RangePtr* ranges, size_t n,
                             bool include_end = true) override;

  Status DeleteBlobFilesInRanges(ColumnFamilyHandle* column_family,
                                 const RangePtr* ranges, size_t n,
                                 bool include_end = true) override;

  using TitanDB::GetOptions;
  Options GetOptions(ColumnFamilyHandle* column_family) const override;

  using TitanDB::SetOptions;
  Status SetOptions(
      ColumnFamilyHandle* column_family,
      const std::unordered_map<std::string, std::string>& new_options) override;

  using TitanDB::GetTitanOptions;
  TitanOptions GetTitanOptions(
      ColumnFamilyHandle* column_family) const override;

  using TitanDB::GetTitanDBOptions;
  TitanDBOptions GetTitanDBOptions() const override;

  using TitanDB::GetProperty;
  bool GetProperty(ColumnFamilyHandle* column_family, const Slice& property,
                   std::string* value) override;

  using TitanDB::GetIntProperty;
  bool GetIntProperty(ColumnFamilyHandle* column_family, const Slice& property,
                      uint64_t* value) override;

  bool initialized() const { return initialized_; }

  void OnFlushCompleted(const FlushJobInfo& flush_job_info);

  void OnCompactionCompleted(const CompactionJobInfo& compaction_job_info);

  void StartBackgroundTasks();

  void TEST_set_initialized(bool _initialized) { initialized_ = _initialized; }

  Status TEST_StartGC(uint32_t column_family_id);
  void TEST_WaitForBackgroundGC();

  Status TEST_PurgeObsoleteFiles();

  int TEST_bg_gc_running() {
    MutexLock l(&mutex_);
    return bg_gc_running_;
  }

  std::shared_ptr<BlobStorage> TEST_GetBlobStorage(
      ColumnFamilyHandle* column_family) {
    MutexLock l(&mutex_);
    return blob_file_set_->GetBlobStorage(column_family->GetID()).lock();
  }

 private:
  class FileManager;
  friend class FileManager;
  friend class BlobGCJobTest;
  friend class BaseDbListener;
  friend class TitanDBTest;
  friend class TitanThreadSafetyTest;
  friend class TitanCompactionFilterFactory;
  friend class TitanCompactionFilter;

  Status OpenImpl(const std::vector<TitanCFDescriptor>& descs,
                  std::vector<ColumnFamilyHandle*>* handles);

  Status ValidateOptions(
      const TitanDBOptions& options,
      const std::vector<TitanCFDescriptor>& column_families) const;

  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* handle,
                 const Slice& key, PinnableSlice* value);

  std::vector<Status> MultiGetImpl(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& handles,
      const std::vector<Slice>& keys, std::vector<std::string>* values);

  Iterator* NewIteratorImpl(const TitanReadOptions& options,
                            ColumnFamilyHandle* handle,
                            std::shared_ptr<ManagedSnapshot> snapshot);

  Status InitializeGC(const std::vector<ColumnFamilyHandle*>& cf_handles);

  Status ExtractGCStatsFromTableProperty(
      const std::shared_ptr<const TableProperties>& table_properties,
      bool to_add, std::map<uint64_t, int64_t>* blob_file_size_diff);

  Status ExtractGCStatsFromTableProperty(
      const TableProperties& table_properties, bool to_add,
      std::map<uint64_t, int64_t>* blob_file_size_diff);

  // REQUIRE: mutex_ held
  void AddToGCQueue(uint32_t column_family_id) {
    mutex_.AssertHeld();
    unscheduled_gc_++;
    gc_queue_.push_back(column_family_id);
  }

  // REQUIRE: gc_queue_ not empty
  // REQUIRE: mutex_ held
  uint32_t PopFirstFromGCQueue() {
    assert(!gc_queue_.empty());
    auto column_family_id = *gc_queue_.begin();
    gc_queue_.pop_front();
    return column_family_id;
  }

  // REQUIRE: mutex_ held
  void MaybeScheduleGC();

  static void BGWorkGC(void* db);
  void BackgroundCallGC();
  Status BackgroundGC(LogBuffer* log_buffer, uint32_t column_family_id);

  void PurgeObsoleteFiles();
  Status PurgeObsoleteFilesImpl();

  SequenceNumber GetOldestSnapshotSequence() {
    SequenceNumber oldest_snapshot = kMaxSequenceNumber;
    {
      // Need to lock DBImpl mutex before access snapshot list.
      InstrumentedMutexLock l(db_impl_->mutex());
      auto& snapshots = db_impl_->snapshots();
      if (!snapshots.empty()) {
        oldest_snapshot = snapshots.oldest()->GetSequenceNumber();
      }
    }
    return oldest_snapshot;
  }

  // REQUIRE: mutex_ held
  bool HasPendingDropCFRequest(uint32_t cf_id);

  // REQUIRE: mutex_ held
  Status SetBGError(const Status& s);

  Status GetBGError() {
    MutexLock l(&mutex_);
    return bg_error_;
  }

  void MarkFileIfNeedMerge(
      const std::vector<std::shared_ptr<BlobFileMeta>>& files,
      int max_sorted_runs);

  bool HasBGError() { return has_bg_error_.load(); }

  void DumpStats();

  FileLock* lock_{nullptr};
  // The lock sequence must be Titan.mutex_.Lock() -> Base DB mutex_.Lock()
  // while the unlock sequence must be Base DB mutex.Unlock() ->
  // Titan.mutex_.Unlock() Only if we all obey these sequence, we can prevent
  // potential dead lock.
  mutable port::Mutex mutex_;
  // This condition variable is signaled on these conditions:
  // * whenever bg_gc_scheduled_ goes down to 0.
  // * whenever bg_gc_running_ goes down to 0.
  // * whenever drop_cf_requests_ goes down to 0.
  port::CondVar bg_cv_;

  std::string dbname_;
  std::string dirname_;
  Env* env_;
  EnvOptions env_options_;
  DBImpl* db_impl_;
  TitanDBOptions db_options_;
  std::unique_ptr<Directory> directory_;
  std::shared_ptr<BlobIndexMergeOperator> shared_merge_operator_;

  std::atomic<bool> initialized_{false};

  // Turn DB into read-only if background error happened
  Status bg_error_;
  std::atomic_bool has_bg_error_{false};

  // Thread pool for running background GC.
  std::unique_ptr<ThreadPool> thread_pool_;

  // TitanStats is turned on only if statistics field of DBOptions
  // is not null.
  std::unique_ptr<TitanStats> stats_;

  // Access while holding mutex_ lock or during DB open.
  std::unordered_map<uint32_t, TitanColumnFamilyInfo> cf_info_;

  // handle for purging obsolete blob files at fixed intervals
  std::unique_ptr<RepeatableThread> thread_purge_obsolete_;

  // handle for dump internal stats at fixed intervals.
  std::unique_ptr<RepeatableThread> thread_dump_stats_;

  std::unique_ptr<BlobFileSet> blob_file_set_;
  std::set<uint64_t> pending_outputs_;
  std::shared_ptr<BlobFileManager> blob_manager_;

  // gc_queue_ hold column families that we need to gc.
  // pending_gc_ hold column families that already on gc_queue_.
  std::deque<uint32_t> gc_queue_;

  // REQUIRE: mutex_ held.
  int bg_gc_scheduled_ = 0;
  // REQUIRE: mutex_ held.
  int bg_gc_running_ = 0;
  // REQUIRE: mutex_ held.
  int unscheduled_gc_ = 0;
  // REQUIRE: mutex_ held.
  int drop_cf_requests_ = 0;

  // PurgeObsoleteFiles, DisableFileDeletions and EnableFileDeletions block
  // on the mutex to avoid contention.
  mutable port::Mutex delete_titandb_file_mutex_;

  // REQUIRES: access with delete_titandb_file_mutex_ held.
  int disable_titandb_file_deletions_ = 0;

  std::atomic_bool shuting_down_{false};
};

}  // namespace titandb
}  // namespace rocksdb
