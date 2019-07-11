#include "db_impl.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "port/port.h"

#include "base_db_listener.h"
#include "blob_file_builder.h"
#include "blob_file_iterator.h"
#include "blob_file_size_collector.h"
#include "blob_gc.h"
#include "db_iter.h"
#include "table_factory.h"
#include "titan_build_version.h"

namespace rocksdb {
namespace titandb {

class TitanDBImpl::FileManager : public BlobFileManager {
 public:
  FileManager(TitanDBImpl* db) : db_(db) {}

  Status NewFile(std::unique_ptr<BlobFileHandle>* handle) override {
    auto number = db_->vset_->NewFileNumber();
    auto name = BlobFileName(db_->dirname_, number);

    Status s;
    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      s = db_->env_->NewWritableFile(name, &f, db_->env_options_);
      if (!s.ok()) return s;
      file.reset(new WritableFileWriter(std::move(f), name, db_->env_options_));
    }

    handle->reset(new FileHandle(number, name, std::move(file)));
    {
      MutexLock l(&db_->mutex_);
      db_->pending_outputs_.insert(number);
    }
    return s;
  }

  Status BatchFinishFiles(
      uint32_t cf_id,
      const std::vector<std::pair<std::shared_ptr<BlobFileMeta>,
                                  std::unique_ptr<BlobFileHandle>>>& files)
      override {
    Status s;
    VersionEdit edit;
    edit.SetColumnFamilyID(cf_id);
    for (auto& file : files) {
      RecordTick(statistics(db_->stats_.get()), BLOB_DB_BLOB_FILE_SYNCED);
      {
        StopWatch sync_sw(db_->env_, statistics(db_->stats_.get()),
                          BLOB_DB_BLOB_FILE_SYNC_MICROS);
        s = file.second->GetFile()->Sync(false);
      }
      if (s.ok()) {
        s = file.second->GetFile()->Close();
      }
      if (!s.ok()) return s;

      ROCKS_LOG_INFO(db_->db_options_.info_log, "Titan adding blob file [%llu]",
                     file.first->file_number());
      edit.AddBlobFile(file.first);
    }

    {
      MutexLock l(&db_->mutex_);
      s = db_->vset_->LogAndApply(edit);
      for (const auto& file : files)
        db_->pending_outputs_.erase(file.second->GetNumber());
    }
    return s;
  }

  Status BatchDeleteFiles(
      const std::vector<std::unique_ptr<BlobFileHandle>>& handles) override {
    Status s;
    uint64_t file_size = 0;
    for (auto& handle : handles) {
      s = db_->env_->DeleteFile(handle->GetName());
      file_size += handle->GetFile()->GetFileSize();
    }
    {
      MutexLock l(&db_->mutex_);
      for (const auto& handle : handles)
        db_->pending_outputs_.erase(handle->GetNumber());
    }
    return s;
  }

 private:
  class FileHandle : public BlobFileHandle {
   public:
    FileHandle(uint64_t number, const std::string& name,
               std::unique_ptr<WritableFileWriter> file)
        : number_(number), name_(name), file_(std::move(file)) {}

    uint64_t GetNumber() const override { return number_; }

    const std::string& GetName() const override { return name_; }

    WritableFileWriter* GetFile() const override { return file_.get(); }

   private:
    uint64_t number_;
    std::string name_;
    std::unique_ptr<WritableFileWriter> file_;
  };

  TitanDBImpl* db_;
};

TitanDBImpl::TitanDBImpl(const TitanDBOptions& options,
                         const std::string& dbname)
    : bg_cv_(&mutex_),
      dbname_(dbname),
      env_(options.env),
      env_options_(options),
      db_options_(options) {
  if (db_options_.dirname.empty()) {
    db_options_.dirname = dbname_ + "/titandb";
  }
  dirname_ = db_options_.dirname;
  if (db_options_.statistics != nullptr) {
    stats_.reset(new TitanStats(db_options_.statistics.get()));
  }
  blob_manager_.reset(new FileManager(this));
}

TitanDBImpl::~TitanDBImpl() { Close(); }

void TitanDBImpl::StartBackgroundTasks() {
  if (!thread_purge_obsolete_) {
    thread_purge_obsolete_.reset(new rocksdb::RepeatableThread(
        [this]() { TitanDBImpl::PurgeObsoleteFiles(); }, "titanbg", env_,
        db_options_.purge_obsolete_files_period * 1000 * 1000));
  }
}

Status TitanDBImpl::Open(const std::vector<TitanCFDescriptor>& descs,
                         std::vector<ColumnFamilyHandle*>* handles) {
  // Sets up directories for base DB and Titan.
  Status s = env_->CreateDirIfMissing(dbname_);
  if (!s.ok()) return s;
  if (!db_options_.info_log) {
    s = CreateLoggerFromOptions(dbname_, db_options_, &db_options_.info_log);
    if (!s.ok()) return s;
  }
  s = env_->CreateDirIfMissing(dirname_);
  if (!s.ok()) return s;
  s = env_->LockFile(LockFileName(dirname_), &lock_);
  if (!s.ok()) return s;

  // Descriptors for initial DB open to get CF ids.
  std::vector<ColumnFamilyDescriptor> init_descs;
  // Descriptors for actually open DB.
  std::vector<ColumnFamilyDescriptor> base_descs;
  for (auto& desc : descs) {
    init_descs.emplace_back(desc.name, desc.options);
    base_descs.emplace_back(desc.name, desc.options);
  }
  std::map<uint32_t, TitanCFOptions> column_families;

  // Opens the base DB first to collect the column families information
  //
  // Disable compaction at this point because we haven't add table properties
  // collector. A compaction can generate a SST file without blob size table
  // property. A later compaction after Titan DB open can cause crash because
  // OnCompactionCompleted use table property to discover blob files generated
  // by the compaction, and get confused by missing property.
  //
  // We also avoid flush here because we haven't replaced the table factory
  // yet, but rocksdb may still flush if memtable is full. This is fine though,
  // since values in memtable are raw values.
  for (auto& desc : init_descs) {
    desc.options.disable_auto_compactions = true;
  }
  db_options_.avoid_flush_during_recovery = true;
  // Add EventListener to collect statistics for GC
  db_options_.listeners.emplace_back(std::make_shared<BaseDbListener>(this));
  // Note that info log is initialized after `CreateLoggerFromOptions`,
  // so new `VersionSet` here but not in constructor is to get a proper info
  // log.
  vset_.reset(new VersionSet(db_options_, stats_.get()));

  s = DB::Open(db_options_, dbname_, init_descs, handles, &db_);
  if (s.ok()) {
    for (size_t i = 0; i < descs.size(); i++) {
      auto handle = (*handles)[i];
      uint32_t cf_id = handle->GetID();
      column_families.emplace(cf_id, descs[i].options);
      db_->DestroyColumnFamilyHandle(handle);
      // Replaces the provided table factory with TitanTableFactory.
      // While we need to preserve original table_factory for GetOptions.
      auto& base_table_factory = base_descs[i].options.table_factory;
      assert(base_table_factory != nullptr);
      immutable_cf_options_.emplace(cf_id,
                                    ImmutableTitanCFOptions(descs[i].options));
      mutable_cf_options_.emplace(cf_id,
                                  MutableTitanCFOptions(descs[i].options));
      base_table_factory_[cf_id] = base_table_factory;
      titan_table_factory_[cf_id] = std::make_shared<TitanTableFactory>(
          db_options_, descs[i].options, blob_manager_, &mutex_, vset_.get(),
          stats_.get());
      base_descs[i].options.table_factory = titan_table_factory_[cf_id];
      // Add TableProperties for collecting statistics GC
      base_descs[i].options.table_properties_collector_factories.emplace_back(
          std::make_shared<BlobFileSizeCollectorFactory>());
    }
    handles->clear();
    s = db_->Close();
    delete db_;
  }
  if (!s.ok()) return s;

  s = vset_->Open(column_families);
  if (!s.ok()) return s;

  static bool has_init_background_threads = false;
  if (!has_init_background_threads) {
    auto bottom_pri_threads_num =
        env_->GetBackgroundThreads(Env::Priority::BOTTOM);
    if (!db_options_.disable_background_gc &&
        db_options_.max_background_gc > 0) {
      env_->IncBackgroundThreadsIfNeeded(
          db_options_.max_background_gc + bottom_pri_threads_num,
          Env::Priority::BOTTOM);
      assert(env_->GetBackgroundThreads(Env::Priority::BOTTOM) ==
             bottom_pri_threads_num + db_options_.max_background_gc);
    }
    has_init_background_threads = true;
  }

  s = DB::Open(db_options_, dbname_, base_descs, handles, &db_);
  if (s.ok()) {
    db_impl_ = reinterpret_cast<DBImpl*>(db_->GetRootDB());
    if (stats_.get()) {
      stats_->Initialize(column_families, db_->DefaultColumnFamily()->GetID());
    }
    ROCKS_LOG_INFO(db_options_.info_log, "Titan DB open.");
    ROCKS_LOG_HEADER(db_options_.info_log, "Titan git sha: %s",
                     titan_build_git_sha);
    db_options_.Dump(db_options_.info_log.get());
    for (auto& desc : descs) {
      ROCKS_LOG_HEADER(db_options_.info_log,
                       "Column family [%s], options:", desc.name.c_str());
      desc.options.Dump(db_options_.info_log.get());
    }
  } else {
    ROCKS_LOG_ERROR(db_options_.info_log, "Titan DB open failed: %s",
                    s.ToString().c_str());
  }
  return s;
}

Status TitanDBImpl::Close() {
  Status s;
  CloseImpl();
  if (db_) {
    s = db_->Close();
    delete db_;
    db_ = nullptr;
    db_impl_ = nullptr;
  }
  if (lock_) {
    env_->UnlockFile(lock_);
    lock_ = nullptr;
  }
  return s;
}

Status TitanDBImpl::CloseImpl() {
  {
    MutexLock l(&mutex_);
    // Although `shuting_down_` is atomic bool object, we should set it under
    // the protection of mutex_, otherwise, there maybe something wrong with it,
    // like:
    // 1, A thread: shuting_down_.load = false
    // 2, B thread: shuting_down_.store(true)
    // 3, B thread: unschedule all bg work
    // 4, A thread: schedule bg work
    shuting_down_.store(true, std::memory_order_release);
  }

  int gc_unscheduled = env_->UnSchedule(this, Env::Priority::BOTTOM);
  {
    MutexLock l(&mutex_);
    bg_gc_scheduled_ -= gc_unscheduled;
    while (bg_gc_scheduled_ > 0) {
      bg_cv_.Wait();
    }
  }

  if (thread_purge_obsolete_ != nullptr) {
    thread_purge_obsolete_->cancel();
    mutex_.Lock();
    thread_purge_obsolete_.reset();
    mutex_.Unlock();
  }

  return Status::OK();
}

Status TitanDBImpl::CreateColumnFamilies(
    const std::vector<TitanCFDescriptor>& descs,
    std::vector<ColumnFamilyHandle*>* handles) {
  std::vector<ColumnFamilyDescriptor> base_descs;
  std::vector<std::shared_ptr<TableFactory>> base_table_factory;
  std::vector<std::shared_ptr<TitanTableFactory>> titan_table_factory;
  for (auto& desc : descs) {
    ColumnFamilyOptions options = desc.options;
    // Replaces the provided table factory with TitanTableFactory.
    base_table_factory.emplace_back(options.table_factory);
    titan_table_factory.emplace_back(std::make_shared<TitanTableFactory>(
        db_options_, desc.options, blob_manager_, &mutex_, vset_.get(),
        stats_.get()));
    options.table_factory = titan_table_factory.back();
    base_descs.emplace_back(desc.name, options);
  }

  Status s = db_impl_->CreateColumnFamilies(base_descs, handles);
  assert(handles->size() == descs.size());

  if (s.ok()) {
    std::map<uint32_t, TitanCFOptions> column_families;
    {
      MutexLock l(&mutex_);
      for (size_t i = 0; i < descs.size(); i++) {
        uint32_t cf_id = (*handles)[i]->GetID();
        column_families.emplace(cf_id, descs[i].options);
        immutable_cf_options_.emplace(
            cf_id, ImmutableTitanCFOptions(descs[i].options));
        mutable_cf_options_.emplace(cf_id,
                                    MutableTitanCFOptions(descs[i].options));
        base_table_factory_[cf_id] = base_table_factory[i];
        titan_table_factory_[cf_id] = titan_table_factory[i];
      }
      vset_->AddColumnFamilies(column_families);
    }
  }
  if (s.ok()) {
    for (auto& desc : descs) {
      ROCKS_LOG_INFO(db_options_.info_log, "Created column family [%s].",
                     desc.name.c_str());
      desc.options.Dump(db_options_.info_log.get());
    }
  } else {
    std::string column_families_str;
    for (auto& desc : descs) {
      column_families_str += "[" + desc.name + "]";
    }
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to create column families %s: %s",
                    column_families_str.c_str(), s.ToString().c_str());
  }
  return s;
}

Status TitanDBImpl::DropColumnFamilies(
    const std::vector<ColumnFamilyHandle*>& handles) {
  std::vector<uint32_t> column_families;
  std::string column_families_str;
  for (auto& handle : handles) {
    column_families.emplace_back(handle->GetID());
    column_families_str += "[" + handle->GetName() + "]";
  }
  Status s = db_impl_->DropColumnFamilies(handles);
  if (s.ok()) {
    MutexLock l(&mutex_);
    for (auto cf_id : column_families) {
      base_table_factory_.erase(cf_id);
      titan_table_factory_.erase(cf_id);
    }
    SequenceNumber obsolete_sequence = db_impl_->GetLatestSequenceNumber();
    s = vset_->DropColumnFamilies(column_families, obsolete_sequence);
  }
  if (s.ok()) {
    ROCKS_LOG_INFO(db_options_.info_log, "Dropped column families: %s",
                   column_families_str.c_str());
  } else {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to drop column families %s: %s",
                    column_families_str.c_str(), s.ToString().c_str());
  }
  return s;
}

Status TitanDBImpl::DestroyColumnFamilyHandle(
    ColumnFamilyHandle* column_family) {
  if (column_family == nullptr) {
    return Status::InvalidArgument("Column family handle is nullptr.");
  }
  auto cf_id = column_family->GetID();
  std::string cf_name = column_family->GetName();
  Status s = db_impl_->DestroyColumnFamilyHandle(column_family);

  if (s.ok()) {
    MutexLock l(&mutex_);
    // it just changes some marks and doesn't delete blob files physically.
    vset_->DestroyColumnFamily(cf_id);
  }
  if (s.ok()) {
    ROCKS_LOG_INFO(db_options_.info_log, "Destroyed column family handle [%s].",
                   cf_name.c_str());
  } else {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to destroy column family handle [%s]: %s",
                    cf_name.c_str(), s.ToString().c_str());
  }
  return s;
}

Status TitanDBImpl::CompactFiles(
    const CompactionOptions& compact_options, ColumnFamilyHandle* column_family,
    const std::vector<std::string>& input_file_names, const int output_level,
    const int output_path_id, std::vector<std::string>* const output_file_names,
    CompactionJobInfo* compaction_job_info) {
  std::unique_ptr<CompactionJobInfo> compaction_job_info_ptr;
  if (compaction_job_info == nullptr) {
    compaction_job_info_ptr.reset(new CompactionJobInfo());
    compaction_job_info = compaction_job_info_ptr.get();
  }
  auto s = db_impl_->CompactFiles(
      compact_options, column_family, input_file_names, output_level,
      output_path_id, output_file_names, compaction_job_info);
  if (s.ok()) {
    OnCompactionCompleted(*compaction_job_info);
  }

  return s;
}

Status TitanDBImpl::Get(const ReadOptions& options, ColumnFamilyHandle* handle,
                        const Slice& key, PinnableSlice* value) {
  if (options.snapshot) {
    return GetImpl(options, handle, key, value);
  }
  ReadOptions ro(options);
  ManagedSnapshot snapshot(this);
  ro.snapshot = snapshot.snapshot();
  return GetImpl(ro, handle, key, value);
}

Status TitanDBImpl::GetImpl(const ReadOptions& options,
                            ColumnFamilyHandle* handle, const Slice& key,
                            PinnableSlice* value) {
  Status s;
  bool is_blob_index = false;
  s = db_impl_->GetImpl(options, handle, key, value, nullptr /*value_found*/,
                        nullptr /*read_callback*/, &is_blob_index);
  if (!s.ok() || !is_blob_index) return s;

  StopWatch get_sw(env_, statistics(stats_.get()), BLOB_DB_GET_MICROS);

  BlobIndex index;
  s = index.DecodeFrom(value);
  assert(s.ok());
  if (!s.ok()) return s;

  BlobRecord record;
  PinnableSlice buffer;

  mutex_.Lock();
  auto storage = vset_->GetBlobStorage(handle->GetID()).lock();
  mutex_.Unlock();

  {
    StopWatch read_sw(env_, statistics(stats_.get()),
                      BLOB_DB_BLOB_FILE_READ_MICROS);
    s = storage->Get(options, index, &record, &buffer);
    RecordTick(statistics(stats_.get()), BLOB_DB_NUM_KEYS_READ);
    RecordTick(statistics(stats_.get()), BLOB_DB_BLOB_FILE_BYTES_READ,
               index.blob_handle.size);
  }
  if (s.IsCorruption()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Key:%s Snapshot:%" PRIu64 " GetBlobFile err:%s\n",
                    key.ToString(true).c_str(),
                    options.snapshot->GetSequenceNumber(),
                    s.ToString().c_str());
  }
  if (s.ok()) {
    value->Reset();
    value->PinSelf(record.value);
  }
  return s;
}

std::vector<Status> TitanDBImpl::MultiGet(
    const ReadOptions& options, const std::vector<ColumnFamilyHandle*>& handles,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  auto options_copy = options;
  options_copy.total_order_seek = true;
  if (options_copy.snapshot) {
    return MultiGetImpl(options_copy, handles, keys, values);
  }
  ReadOptions ro(options_copy);
  ManagedSnapshot snapshot(this);
  ro.snapshot = snapshot.snapshot();
  return MultiGetImpl(ro, handles, keys, values);
}

std::vector<Status> TitanDBImpl::MultiGetImpl(
    const ReadOptions& options, const std::vector<ColumnFamilyHandle*>& handles,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  std::vector<Status> res;
  res.resize(keys.size());
  values->resize(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    auto value = &(*values)[i];
    PinnableSlice pinnable_value(value);
    res[i] = GetImpl(options, handles[i], keys[i], &pinnable_value);
    if (res[i].ok() && pinnable_value.IsPinned()) {
      value->assign(pinnable_value.data(), pinnable_value.size());
    }
  }
  return res;
}

Iterator* TitanDBImpl::NewIterator(const TitanReadOptions& options,
                                   ColumnFamilyHandle* handle) {
  TitanReadOptions options_copy = options;
  options_copy.total_order_seek = true;
  std::shared_ptr<ManagedSnapshot> snapshot;
  if (options_copy.snapshot) {
    return NewIteratorImpl(options_copy, handle, snapshot);
  }
  TitanReadOptions ro(options_copy);
  snapshot.reset(new ManagedSnapshot(this));
  ro.snapshot = snapshot->snapshot();
  return NewIteratorImpl(ro, handle, snapshot);
}

Iterator* TitanDBImpl::NewIteratorImpl(
    const TitanReadOptions& options, ColumnFamilyHandle* handle,
    std::shared_ptr<ManagedSnapshot> snapshot) {
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(handle)->cfd();

  mutex_.Lock();
  auto storage = vset_->GetBlobStorage(handle->GetID());
  mutex_.Unlock();

  std::unique_ptr<ArenaWrappedDBIter> iter(db_impl_->NewIteratorImpl(
      options, cfd, options.snapshot->GetSequenceNumber(),
      nullptr /*read_callback*/, true /*allow_blob*/, true /*allow_refresh*/));
  return new TitanDBIterator(options, storage.lock().get(), snapshot,
                             std::move(iter), env_, stats_.get(),
                             db_options_.info_log.get());
}

Status TitanDBImpl::NewIterators(
    const TitanReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& handles,
    std::vector<Iterator*>* iterators) {
  TitanReadOptions ro(options);
  ro.total_order_seek = true;
  std::shared_ptr<ManagedSnapshot> snapshot;
  if (!ro.snapshot) {
    snapshot.reset(new ManagedSnapshot(this));
    ro.snapshot = snapshot->snapshot();
  }
  iterators->clear();
  iterators->reserve(handles.size());
  for (auto& handle : handles) {
    iterators->emplace_back(NewIteratorImpl(ro, handle, snapshot));
  }
  return Status::OK();
}

const Snapshot* TitanDBImpl::GetSnapshot() { return db_->GetSnapshot(); }

void TitanDBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  // TODO:
  // We can record here whether the oldest snapshot is released.
  // If not, we can just skip the next round of purging obsolete files.
  db_->ReleaseSnapshot(snapshot);
}

Options TitanDBImpl::GetOptions(ColumnFamilyHandle* column_family) const {
  assert(column_family != nullptr);
  Options options = db_->GetOptions(column_family);
  uint32_t cf_id = column_family->GetID();

  MutexLock l(&mutex_);
  if (base_table_factory_.count(cf_id) > 0) {
    options.table_factory = base_table_factory_.at(cf_id);
  } else {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "Failed to get original table factory for column family %s.",
        column_family->GetName().c_str());
    options.table_factory.reset();
  }
  return options;
}

Status TitanDBImpl::SetOptions(
    ColumnFamilyHandle* column_family,
    const std::unordered_map<std::string, std::string>& new_options) {
  Status s;
  auto opts = new_options;
  auto p = opts.find("blob_run_mode");
  bool set_blob_run_mode = (p != opts.end());
  TitanBlobRunMode mode = TitanBlobRunMode::kNormal;
  if (set_blob_run_mode) {
    const std::string& blob_run_mode_string = p->second;
    auto pm = blob_run_mode_string_map.find(blob_run_mode_string);
    if (pm == blob_run_mode_string_map.end()) {
      return Status::InvalidArgument("No blob_run_mode defined for " +
                                     blob_run_mode_string);
    } else {
      mode = pm->second;
      ROCKS_LOG_INFO(db_options_.info_log, "[%s] Set blob_run_mode: %s",
                     column_family->GetName().c_str(),
                     blob_run_mode_string.c_str());
    }
    opts.erase(p);
  }
  if (opts.size() > 0) {
    s = db_->SetOptions(column_family, opts);
    if (!s.ok()) {
      return s;
    }
  }
  // Make sure base db's SetOptions sucesss before setting blob_run_mode.
  if (set_blob_run_mode) {
    uint32_t cf_id = column_family->GetID();
    {
      MutexLock l(&mutex_);
      auto& table_factory = titan_table_factory_[cf_id];
      table_factory->SetBlobRunMode(mode);
      mutable_cf_options_[cf_id].blob_run_mode = mode;
    }
  }
  return Status::OK();
}

TitanOptions TitanDBImpl::GetTitanOptions(
    ColumnFamilyHandle* column_family) const {
  assert(column_family != nullptr);
  Options base_options = GetOptions(column_family);
  TitanOptions titan_options;
  *static_cast<TitanDBOptions*>(&titan_options) = db_options_;
  *static_cast<DBOptions*>(&titan_options) =
      static_cast<DBOptions>(base_options);
  uint32_t cf_id = column_family->GetID();
  {
    MutexLock l(&mutex_);
    *static_cast<TitanCFOptions*>(&titan_options) = TitanCFOptions(
        static_cast<ColumnFamilyOptions>(base_options),
        immutable_cf_options_.at(cf_id), mutable_cf_options_.at(cf_id));
  }
  return titan_options;
}

TitanDBOptions TitanDBImpl::GetTitanDBOptions() const {
  // Titan db_options_ is not mutable after DB open.
  TitanDBOptions result = db_options_;
  *static_cast<DBOptions*>(&result) = db_impl_->GetDBOptions();
  return result;
}

bool TitanDBImpl::GetProperty(ColumnFamilyHandle* column_family,
                              const Slice& property, std::string* value) {
  assert(column_family != nullptr);
  bool s = false;
  if (stats_.get() != nullptr) {
    auto stats = stats_->internal_stats(column_family->GetID());
    if (stats != nullptr) {
      s = stats->GetStringProperty(property, value);
    }
  }
  if (s) {
    return s;
  } else {
    return db_impl_->GetProperty(column_family, property, value);
  }
}

bool TitanDBImpl::GetIntProperty(ColumnFamilyHandle* column_family,
                                 const Slice& property, uint64_t* value) {
  assert(column_family != nullptr);
  bool s = false;
  if (stats_.get() != nullptr) {
    auto stats = stats_->internal_stats(column_family->GetID());
    if (stats != nullptr) {
      s = stats->GetIntProperty(property, value);
    }
  }
  if (s) {
    return s;
  } else {
    return db_impl_->GetIntProperty(column_family, property, value);
  }
}

void TitanDBImpl::OnFlushCompleted(const FlushJobInfo& flush_job_info) {
  const auto& tps = flush_job_info.table_properties;
  auto ucp_iter = tps.user_collected_properties.find(
      BlobFileSizeCollector::kPropertiesName);
  // sst file doesn't contain any blob index
  if (ucp_iter == tps.user_collected_properties.end()) {
    return;
  }
  std::map<uint64_t, uint64_t> blob_files_size;
  Slice src{ucp_iter->second};
  if (!BlobFileSizeCollector::Decode(&src, &blob_files_size)) {
    // TODO: Should treat it as background error and make DB read-only.
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "OnFlushCompleted[%d]: failed to decode table property, "
                    "prroperty size: %" ROCKSDB_PRIszt ".",
                    flush_job_info.job_id, ucp_iter->second.size());
    assert(false);
  }
  assert(!blob_files_size.empty());
  std::set<uint64_t> outputs;
  for (const auto f : blob_files_size) {
    outputs.insert(f.first);
  }

  {
    MutexLock l(&mutex_);
    auto blob_storage = vset_->GetBlobStorage(flush_job_info.cf_id).lock();
    if (!blob_storage) {
      // TODO: Should treat it as background error and make DB read-only.
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "OnFlushCompleted[%d]: Column family id: %" PRIu32
                      " Not Found.",
                      flush_job_info.job_id, flush_job_info.cf_id);
      assert(false);
      return;
    }
    for (const auto& file_number : outputs) {
      auto file = blob_storage->FindFile(file_number).lock();
      // This file maybe output of a gc job, and it's been gced out.
      if (!file) {
        continue;
      }
      ROCKS_LOG_INFO(db_options_.info_log,
                     "OnFlushCompleted[%d]: output blob file %" PRIu64 ".",
                     flush_job_info.job_id, file->file_number());
      file->FileStateTransit(BlobFileMeta::FileEvent::kFlushCompleted);
    }
  }
}

void TitanDBImpl::OnCompactionCompleted(
    const CompactionJobInfo& compaction_job_info) {
  if (!compaction_job_info.status.ok()) {
    // TODO: Clean up blob file generated by the failed compaction.
    return;
  }
  std::map<uint64_t, int64_t> blob_files_size;
  std::set<uint64_t> outputs;
  std::set<uint64_t> inputs;
  auto calc_bfs = [&](const std::vector<std::string>& files, int coefficient,
                      bool output) {
    for (const auto& file : files) {
      auto tp_iter = compaction_job_info.table_properties.find(file);
      if (tp_iter == compaction_job_info.table_properties.end()) {
        if (output) {
          ROCKS_LOG_WARN(
              db_options_.info_log,
              "OnCompactionCompleted[%d]: No table properties for file %s.",
              compaction_job_info.job_id, file.c_str());
        }
        continue;
      }
      auto ucp_iter = tp_iter->second->user_collected_properties.find(
          BlobFileSizeCollector::kPropertiesName);
      // this sst file doesn't contain any blob index
      if (ucp_iter == tp_iter->second->user_collected_properties.end()) {
        continue;
      }
      std::map<uint64_t, uint64_t> input_blob_files_size;
      std::string s = ucp_iter->second;
      Slice slice{s};
      if (!BlobFileSizeCollector::Decode(&slice, &input_blob_files_size)) {
        // TODO: Should treat it as background error and make DB read-only.
        ROCKS_LOG_ERROR(
            db_options_.info_log,
            "OnCompactionCompleted[%d]: failed to decode table property, "
            "compaction file: %s, property size: %" ROCKSDB_PRIszt ".",
            compaction_job_info.job_id, file.c_str(), s.size());
        assert(false);
        continue;
      }
      for (const auto& input_bfs : input_blob_files_size) {
        if (output) {
          if (inputs.find(input_bfs.first) == inputs.end()) {
            outputs.insert(input_bfs.first);
          }
        } else {
          inputs.insert(input_bfs.first);
        }
        auto bfs_iter = blob_files_size.find(input_bfs.first);
        if (bfs_iter == blob_files_size.end()) {
          blob_files_size[input_bfs.first] = coefficient * input_bfs.second;
        } else {
          bfs_iter->second += coefficient * input_bfs.second;
        }
      }
    }
  };

  calc_bfs(compaction_job_info.input_files, -1, false);
  calc_bfs(compaction_job_info.output_files, 1, true);

  {
    MutexLock l(&mutex_);
    auto bs = vset_->GetBlobStorage(compaction_job_info.cf_id).lock();
    if (!bs) {
      // TODO: Should treat it as background error and make DB read-only.
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "OnCompactionCompleted[%d] Column family id:% " PRIu32
                      " not Found.",
                      compaction_job_info.job_id, compaction_job_info.cf_id);
      return;
    }
    for (const auto& file_number : outputs) {
      auto file = bs->FindFile(file_number).lock();
      if (!file) {
        // TODO: Should treat it as background error and make DB read-only.
        ROCKS_LOG_ERROR(
            db_options_.info_log,
            "OnCompactionCompleted[%d]: Failed to get file %" PRIu64,
            compaction_job_info.job_id, file_number);
        assert(false);
        return;
      }
      ROCKS_LOG_INFO(
          db_options_.info_log,
          "OnCompactionCompleted[%d]: compaction output blob file %" PRIu64 ".",
          compaction_job_info.job_id, file->file_number());
      file->FileStateTransit(BlobFileMeta::FileEvent::kCompactionCompleted);
    }

    uint64_t delta = 0;
    for (const auto& bfs : blob_files_size) {
      // blob file size < 0 means discardable size > 0
      if (bfs.second >= 0) {
        continue;
      }
      auto file = bs->FindFile(bfs.first).lock();
      if (!file) {
        // file has been gc out
        continue;
      }
      if (!file->is_obsolete()) {
        delta += -bfs.second;
      }
      file->AddDiscardableSize(static_cast<uint64_t>(-bfs.second));
    }
    SubStats(stats_.get(), compaction_job_info.cf_id,
             TitanInternalStats::LIVE_BLOB_SIZE, delta);
    bs->ComputeGCScore();

    AddToGCQueue(compaction_job_info.cf_id);
    MaybeScheduleGC();
  }
}

}  // namespace titandb
}  // namespace rocksdb
