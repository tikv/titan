#include "db_impl.h"

#include "blob_file_iterator.h"
#include "blob_gc_job.h"
#include "blob_gc_picker.h"

namespace rocksdb {
namespace titandb {

void TitanDBImpl::MaybeScheduleGC() {
  mutex_.AssertHeld();

  if (db_options_.disable_background_gc) return;

  if (shuting_down_.load(std::memory_order_acquire)) return;

  if (bg_gc_scheduled_ >= db_options_.max_background_gc) return;

  bg_gc_scheduled_++;

  env_->Schedule(&TitanDBImpl::BGWorkGC, this, Env::Priority::BOTTOM, this);
}

void TitanDBImpl::BGWorkGC(void* db) {
  reinterpret_cast<TitanDBImpl*>(db)->BackgroundCallGC();
}

void TitanDBImpl::BackgroundCallGC() {
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options_.info_log.get());

  {
    MutexLock l(&mutex_);
    assert(bg_gc_scheduled_ > 0);

    BackgroundGC(&log_buffer);

    {
      mutex_.Unlock();
      log_buffer.FlushBufferToLog();
      LogFlush(db_options_.info_log.get());
      mutex_.Lock();
    }

    bg_gc_scheduled_--;
    if (bg_gc_scheduled_ == 0) {
      // signal if
      // * bg_gc_scheduled_ == 0 -- need to wakeup ~TitanDBImpl
      // If none of this is true, there is no need to signal since nobody is
      // waiting for it
      bg_cv_.SignalAll();
    }
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be deallocated and referencing them
    // will cause trouble.
  }
}

Status TitanDBImpl::BackgroundGC(LogBuffer* log_buffer) {
  mutex_.AssertHeld();
  StopWatch gc_sw(env_, statistics(stats_.get()), BLOB_DB_GC_MICROS);

  std::unique_ptr<BlobGC> blob_gc;
  std::unique_ptr<ColumnFamilyHandle> cfh;
  Status s;
  if (!gc_queue_.empty()) {
    uint32_t column_family_id = PopFirstFromGCQueue();
    auto bs = vset_->GetBlobStorage(column_family_id).lock().get();

    if (bs) {
      const auto& cf_options = bs->cf_options();
      std::shared_ptr<BlobGCPicker> blob_gc_picker =
          std::make_shared<BasicBlobGCPicker>(db_options_, cf_options);
      blob_gc = blob_gc_picker->PickBlobGC(bs);

      if (blob_gc) {
        cfh = db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);
        assert(column_family_id == cfh->GetID());
        blob_gc->SetColumnFamily(cfh.get());
      }
    }
  }

  // TODO(@DorianZheng) Make sure enough room for GC

  if (UNLIKELY(!blob_gc)) {
    // Nothing to do
    ROCKS_LOG_BUFFER(log_buffer, "Titan GC nothing to do");
  } else {
    BlobGCJob blob_gc_job(blob_gc.get(), db_, &mutex_, db_options_, env_,
                          env_options_, blob_manager_.get(), vset_.get(),
                          log_buffer, &shuting_down_, stats_.get());
    s = blob_gc_job.Prepare();
    if (s.ok()) {
      mutex_.Unlock();
      s = blob_gc_job.Run();
      mutex_.Lock();
    }
    if (s.ok()) {
      s = blob_gc_job.Finish();
    }
    blob_gc->ReleaseGcFiles();
  }

  if (s.ok()) {
    // Done
  } else {
    SetBGError(s);
    ROCKS_LOG_WARN(db_options_.info_log, "Titan GC error: %s",
                   s.ToString().c_str());
  }

  return s;
}

Status TitanDBImpl::TEST_StartGC(uint32_t column_family_id) {
  {
    MutexLock l(&mutex_);
    bg_gc_scheduled_++;
  }
  // BackgroundCallGC
  Status s;
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options_.info_log.get());
  {
    MutexLock l(&mutex_);
    assert(bg_gc_scheduled_ > 0);

    // BackgroudGC
    StopWatch gc_sw(env_, statistics(stats_.get()), BLOB_DB_GC_MICROS);

    std::unique_ptr<BlobGC> blob_gc;
    std::unique_ptr<ColumnFamilyHandle> cfh;

    auto bs = vset_->GetBlobStorage(column_family_id).lock().get();
    const auto& cf_options = bs->cf_options();
    std::shared_ptr<BlobGCPicker> blob_gc_picker =
        std::make_shared<BasicBlobGCPicker>(db_options_, cf_options);
    blob_gc = blob_gc_picker->PickBlobGC(bs);

    if (blob_gc) {
      cfh = db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);
      assert(column_family_id == cfh->GetID());
      blob_gc->SetColumnFamily(cfh.get());
    }

    if (UNLIKELY(!blob_gc)) {
      ROCKS_LOG_BUFFER(&log_buffer, "Titan GC nothing to do");
    } else {
      BlobGCJob blob_gc_job(blob_gc.get(), db_, &mutex_, db_options_, env_,
                            env_options_, blob_manager_.get(), vset_.get(),
                            &log_buffer, &shuting_down_, stats_.get());
      s = blob_gc_job.Prepare();

      if (s.ok()) {
        mutex_.Unlock();
        s = blob_gc_job.Run();
        mutex_.Lock();
      }

      if (s.ok()) {
        s = blob_gc_job.Finish();
      }

      blob_gc->ReleaseGcFiles();
    }

    if (!s.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log, "Titan GC error: %s",
                     s.ToString().c_str());
    }

    {
      mutex_.Unlock();
      log_buffer.FlushBufferToLog();
      LogFlush(db_options_.info_log.get());
      mutex_.Lock();
    }

    bg_gc_scheduled_--;
    if (bg_gc_scheduled_ == 0) {
      bg_cv_.SignalAll();
    }
  }
  return s;
}

}  // namespace titandb
}  // namespace rocksdb
