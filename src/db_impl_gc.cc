#include "test_util/sync_point.h"

#include "blob_file_iterator.h"
#include "blob_file_size_collector.h"
#include "blob_gc_job.h"
#include "blob_gc_picker.h"
#include "db/version_set.h"
#include "db_impl.h"
#include "titan_logging.h"
#include "util.h"

namespace rocksdb {
namespace titandb {

Status TitanDBImpl::ExtractGCStatsFromTableProperty(
    const std::shared_ptr<const TableProperties>& table_properties, bool to_add,
    std::map<uint64_t, int64_t>* blob_file_size_diff) {
  assert(blob_file_size_diff != nullptr);
  if (table_properties == nullptr) {
    // No table property found. File may not contain blob indices.
    return Status::OK();
  }
  return ExtractGCStatsFromTableProperty(*table_properties.get(), to_add,
                                         blob_file_size_diff);
}

Status TitanDBImpl::ExtractGCStatsFromTableProperty(
    const TableProperties& table_properties, bool to_add,
    std::map<uint64_t, int64_t>* blob_file_size_diff) {
  assert(blob_file_size_diff != nullptr);
  auto& prop = table_properties.user_collected_properties;
  auto prop_iter = prop.find(BlobFileSizeCollector::kPropertiesName);
  if (prop_iter == prop.end()) {
    // No table property found. File may not contain blob indices.
    return Status::OK();
  }
  Slice prop_slice(prop_iter->second);
  std::map<uint64_t, uint64_t> blob_file_sizes;
  if (!BlobFileSizeCollector::Decode(&prop_slice, &blob_file_sizes)) {
    return Status::Corruption("Failed to decode blob file size property.");
  }
  for (const auto& blob_file_size : blob_file_sizes) {
    uint64_t file_number = blob_file_size.first;
    int64_t diff = static_cast<int64_t>(blob_file_size.second);
    if (!to_add) {
      diff = -diff;
    }
    (*blob_file_size_diff)[file_number] += diff;
  }
  return Status::OK();
}

Status TitanDBImpl::AsyncInitializeGC(
    const std::vector<ColumnFamilyHandle*>& cf_handles) {
  std::vector<std::pair<uint32_t, Version*>> cfs;
  FlushOptions flush_opts;
  flush_opts.wait = true;
  for (ColumnFamilyHandle* cf_handle : cf_handles) {
    std::shared_ptr<BlobStorage> blob_storage =
        blob_file_set_->GetBlobStorage(cf_handle->GetID()).lock();
    assert(blob_storage != nullptr);
    blob_storage->StartInitializeAllFiles();

    // Flush memtable to make sure keys written by GC are all in SSTs.
    auto s = Flush(flush_opts, cf_handle);
    if (!s.ok()) {
      return s;
    }

    // Hold the version and increment the ref count
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(cf_handle);
    auto cfd = cfh->cfd();
    db_impl_->mutex()->Lock();
    auto version = cfd->current();
    version->Ref();
    db_impl_->mutex()->Unlock();

    cfs.push_back(std::make_pair(cf_handle->GetID(), version));
  }

  thread_initialize_gc_.reset(new port::Thread([=]() {
    Status s;

    auto unref = [=](Version* version) {
      db_impl_->mutex()->Lock();
      version->Unref();
      db_impl_->mutex()->Unlock();
    };

    TEST_SYNC_POINT_CALLBACK("TitanDBImpl::AsyncInitializeGC:Begin", this);
    for (auto cf : cfs) {
      if (shuting_down_.load(std::memory_order_acquire)) {
        unref(cf.second);
        continue;
      }
      auto cf_handle = db_impl_->GetColumnFamilyHandleUnlocked(cf.first);
      if (!cf_handle) {
        unref(cf.second);
        continue;
      }
      TITAN_LOG_INFO(db_options_.info_log,
                     "Titan begin async GC initialization on cf [%s]",
                     cf_handle->GetName().c_str());
      TablePropertiesCollection collection;
      // this operation may be slow
      s = cf.second->GetPropertiesOfAllTables(ReadOptions(), &collection);
      unref(cf.second);
      if (!s.ok()) {
        MutexLock l(&mutex_);
        this->SetBGError(s);
        return;
      }

      std::map<uint64_t, int64_t> blob_file_size_diff;
      for (auto& file : collection) {
        s = ExtractGCStatsFromTableProperty(file.second, true /*to_add*/,
                                            &blob_file_size_diff);
        if (!s.ok()) {
          MutexLock l(&mutex_);
          this->SetBGError(s);
          return;
        }
      }

      mutex_.Lock();
      std::shared_ptr<BlobStorage> blob_storage =
          blob_file_set_->GetBlobStorage(cf_handle->GetID()).lock();
      mutex_.Unlock();
      assert(blob_storage != nullptr);
      for (auto& file_size : blob_file_size_diff) {
        assert(file_size.second >= 0);
        std::shared_ptr<BlobFileMeta> file =
            blob_storage->FindFile(file_size.first).lock();
        if (file != nullptr) {
          file->UpdateLiveDataSize(file_size.second);
        }
      }
      blob_storage->InitializeAllFiles();
      TITAN_LOG_INFO(db_options_.info_log,
                     "Titan finish async GC initialization on cf [%s]",
                     cf_handle->GetName().c_str());
    }

    if (!shuting_down_.load(std::memory_order_acquire)) {
      TEST_SYNC_POINT_CALLBACK(
          "TitanDBImpl::AsyncInitializeGC:BeforeSetInitialized", this);
      // Initialization done.
      initialized_.store(true, std::memory_order_release);
      {
        MutexLock l(&mutex_);
        for (auto cf : cfs) {
          std::shared_ptr<BlobStorage> blob_storage =
              blob_file_set_->GetBlobStorage(cf.first).lock();
          blob_storage->ComputeGCScore();
          AddToGCQueue(cf.first);
        }
        MaybeScheduleGC();
      }
    }
    TEST_SYNC_POINT_CALLBACK("TitanDBImpl::AsyncInitializeGC:End", this);
  }));

  return Status::OK();
}

void TitanDBImpl::MaybeScheduleGC() {
  mutex_.AssertHeld();

  if (db_options_.disable_background_gc) return;

  if (!initialized_.load(std::memory_order_acquire)) return;

  if (shuting_down_.load(std::memory_order_acquire)) return;

  while (unscheduled_gc_ > 0 &&
         bg_gc_scheduled_ < db_options_.max_background_gc) {
    unscheduled_gc_--;
    bg_gc_scheduled_++;
    thread_pool_->SubmitJob(std::bind(&TitanDBImpl::BGWorkGC, this));
  }
}

void TitanDBImpl::BGWorkGC(void* db) {
  reinterpret_cast<TitanDBImpl*>(db)->BackgroundCallGC();
}

void TitanDBImpl::BackgroundCallGC() {
  TEST_SYNC_POINT("TitanDBImpl::BackgroundCallGC:BeforeGCRunning");
  {
    MutexLock l(&mutex_);
    assert(bg_gc_scheduled_ > 0);
    while (drop_cf_requests_ > 0) {
      bg_cv_.Wait();
    }
    bg_gc_running_++;

    TEST_SYNC_POINT("TitanDBImpl::BackgroundCallGC:BeforeBackgroundGC");
    if (!gc_queue_.empty()) {
      uint32_t column_family_id = PopFirstFromGCQueue();
      LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                           db_options_.info_log.get());
      BackgroundGC(&log_buffer, column_family_id);
      {
        mutex_.Unlock();
        log_buffer.FlushBufferToLog();
        LogFlush(db_options_.info_log.get());
        mutex_.Lock();
      }
    }

    bg_gc_running_--;
    bg_gc_scheduled_--;
    MaybeScheduleGC();
    if (bg_gc_scheduled_ == 0 || bg_gc_running_ == 0) {
      // Signal DB destructor if bg_gc_scheduled_ drop to 0.
      // Signal drop CF requests if bg_gc_running_ drop to 0.
      // If none of this is true, there is no need to signal since nobody is
      // waiting for it.
      bg_cv_.SignalAll();
    }
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be deallocated and referencing them
    // will cause trouble.
  }
}

Status TitanDBImpl::BackgroundGC(LogBuffer* log_buffer,
                                 uint32_t column_family_id) {
  mutex_.AssertHeld();

  std::unique_ptr<BlobGC> blob_gc;
  std::unique_ptr<ColumnFamilyHandle> cfh;

  std::shared_ptr<BlobStorage> blob_storage;
  // Skip CFs that have been dropped.
  if (!blob_file_set_->IsColumnFamilyObsolete(column_family_id)) {
    blob_storage = blob_file_set_->GetBlobStorage(column_family_id).lock();
  } else {
    TEST_SYNC_POINT_CALLBACK("TitanDBImpl::BackgroundGC:CFDropped", nullptr);
    TITAN_LOG_BUFFER(log_buffer, "GC skip dropped colum family [%s].",
                     cf_info_[column_family_id].name.c_str());
  }
  if (blob_storage != nullptr) {
    const auto& cf_options = blob_storage->cf_options();
    std::shared_ptr<BlobGCPicker> blob_gc_picker =
        std::make_shared<BasicBlobGCPicker>(db_options_, cf_options,
                                            stats_.get());
    blob_gc = blob_gc_picker->PickBlobGC(blob_storage.get());

    if (blob_gc) {
      cfh = db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);
      assert(column_family_id == cfh->GetID());
      blob_gc->SetColumnFamily(cfh.get());
    }
  }

  Status s;
  // TODO(@DorianZheng) Make sure enough room for GC
  if (UNLIKELY(!blob_gc)) {
    RecordTick(statistics(stats_.get()), TITAN_GC_NO_NEED, 1);
    // Nothing to do
    TITAN_LOG_BUFFER(log_buffer, "Titan GC nothing to do");
  } else {
    StopWatch gc_sw(env_->GetSystemClock().get(), statistics(stats_.get()),
                    TITAN_GC_MICROS);
    BlobGCJob blob_gc_job(blob_gc.get(), db_, &mutex_, db_options_, env_,
                          env_options_, blob_manager_.get(),
                          blob_file_set_.get(), log_buffer, &shuting_down_,
                          stats_.get());
    s = blob_gc_job.Prepare();
    if (s.ok()) {
      mutex_.Unlock();
      TEST_SYNC_POINT("TitanDBImpl::BackgroundGC::BeforeRunGCJob");
      s = blob_gc_job.Run();
      TEST_SYNC_POINT("TitanDBImpl::BackgroundGC::AfterRunGCJob");
      mutex_.Lock();
    }
    if (s.ok()) {
      s = blob_gc_job.Finish();
    }
    blob_gc->ReleaseGcFiles();

    if (blob_gc->trigger_next() &&
        (bg_gc_scheduled_ - 1 + gc_queue_.size() <
         2 * static_cast<uint32_t>(db_options_.max_background_gc))) {
      RecordTick(statistics(stats_.get()), TITAN_GC_TRIGGER_NEXT, 1);
      // There is still data remained to be GCed
      // and the queue is not overwhelmed
      // then put this cf to GC queue for next GC
      AddToGCQueue(blob_gc->column_family_handle()->GetID());
    }

    if (s.ok()) {
      RecordTick(statistics(stats_.get()), TITAN_GC_SUCCESS, 1);
      // Done
    } else {
      SetBGError(s);
      RecordTick(statistics(stats_.get()), TITAN_GC_FAILURE, 1);
      TITAN_LOG_WARN(db_options_.info_log, "Titan GC error: %s",
                     s.ToString().c_str());
    }
  }

  TEST_SYNC_POINT("TitanDBImpl::BackgroundGC:Finish");
  return s;
}

Status TitanDBImpl::TEST_StartGC(uint32_t column_family_id) {
  // BackgroundCallGC
  Status s;
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options_.info_log.get());
  {
    MutexLock l(&mutex_);
    // Prevent CF being dropped while GC is running.
    while (drop_cf_requests_ > 0) {
      bg_cv_.Wait();
    }
    bg_gc_running_++;
    bg_gc_scheduled_++;

    s = BackgroundGC(&log_buffer, column_family_id);

    {
      mutex_.Unlock();
      log_buffer.FlushBufferToLog();
      LogFlush(db_options_.info_log.get());
      mutex_.Lock();
    }

    bg_gc_running_--;
    bg_gc_scheduled_--;
    if (bg_gc_scheduled_ == 0 || bg_gc_running_ == 0) {
      bg_cv_.SignalAll();
    }
  }
  return s;
}

void TitanDBImpl::TEST_WaitForBackgroundGC() {
  MutexLock l(&mutex_);
  while (bg_gc_scheduled_ > 0) {
    bg_cv_.Wait();
  }
}

}  // namespace titandb
}  // namespace rocksdb
