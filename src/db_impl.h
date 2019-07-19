#pragma once

#include "blob_file_manager.h"
#include "db/db_impl.h"
#include "rocksdb/statistics.h"
#include "table_factory.h"
#include "titan/db.h"
#include "util/repeatable_thread.h"
#include "version_set.h"

namespace rocksdb {
namespace titandb {

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

  void OnFlushCompleted(const FlushJobInfo& flush_job_info);

  void OnCompactionCompleted(const CompactionJobInfo& compaction_job_info);

  void StartBackgroundTasks();

  Status TEST_StartGC(uint32_t column_family_id);
  Status TEST_PurgeObsoleteFiles();

 private:
  class FileManager;
  friend class FileManager;
  friend class BlobGCJobTest;
  friend class BaseDbListener;
  friend class TitanDBTest;
  friend class TitanThreadSafetyTest;

  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* handle,
                 const Slice& key, PinnableSlice* value);

  std::vector<Status> MultiGetImpl(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& handles,
      const std::vector<Slice>& keys, std::vector<std::string>* values);

  Iterator* NewIteratorImpl(const TitanReadOptions& options,
                            ColumnFamilyHandle* handle,
                            std::shared_ptr<ManagedSnapshot> snapshot);

  // REQUIRE: mutex_ held
  void AddToGCQueue(uint32_t column_family_id) {
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
  Status BackgroundGC(LogBuffer* log_buffer);

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
  Status SetBGError(const Status& s);

  Status GetBGError() {
    MutexLock l(&mutex_);
    return bg_error_;
  }

  bool HasBGError() { return has_bg_error_.load(); }

  FileLock* lock_{nullptr};
  // The lock sequence must be Titan.mutex_.Lock() -> Base DB mutex_.Lock()
  // while the unlock sequence must be Base DB mutex.Unlock() ->
  // Titan.mutex_.Unlock() Only if we all obey these sequence, we can prevent
  // potential dead lock.
  mutable port::Mutex mutex_;
  // This condition variable is signaled on these conditions:
  // * whenever bg_gc_scheduled_ goes down to 0
  port::CondVar bg_cv_;

  std::string dbname_;
  std::string dirname_;
  Env* env_;
  EnvOptions env_options_;
  DBImpl* db_impl_;
  TitanDBOptions db_options_;
  // Turn DB into read-only if background error happened
  Status bg_error_;
  std::atomic_bool has_bg_error_{false};

  // TitanStats is turned on only if statistics field of DBOptions
  // is not null.
  std::unique_ptr<TitanStats> stats_;

  // Guarded by mutex_.
  std::unordered_map<uint32_t, ImmutableTitanCFOptions> immutable_cf_options_;

  // Guarded by mutex_.
  std::unordered_map<uint32_t, MutableTitanCFOptions> mutable_cf_options_;

  // Guarded by mutex_.
  std::unordered_map<uint32_t, std::shared_ptr<TableFactory>>
      base_table_factory_;

  // Guarded by mutex_.
  std::unordered_map<uint32_t, std::shared_ptr<TitanTableFactory>>
      titan_table_factory_;

  // handle for purging obsolete blob files at fixed intervals
  std::unique_ptr<RepeatableThread> thread_purge_obsolete_;

  std::unique_ptr<VersionSet> vset_;
  std::set<uint64_t> pending_outputs_;
  std::shared_ptr<BlobFileManager> blob_manager_;

  // gc_queue_ hold column families that we need to gc.
  // pending_gc_ hold column families that already on gc_queue_.
  std::deque<uint32_t> gc_queue_;

  std::atomic_int bg_gc_scheduled_{0};
  // REQUIRE: mutex_ held
  int unscheduled_gc_{0};

  std::atomic_bool shuting_down_{false};
};

}  // namespace titandb
}  // namespace rocksdb
