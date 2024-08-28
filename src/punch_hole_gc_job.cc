#include "punch_hole_gc_job.h"

#include <fcntl.h>
#include <unistd.h>

#include "db/db_impl/db_impl.h"

#include "blob_file_iterator.h"
#include "blob_file_reader.h"
#include "titan_logging.h"

namespace rocksdb {
namespace titandb {

Status PunchHoleGCJob::Run() {
  Status s;
  auto cfh = base_db_impl_->GetColumnFamilyHandleUnlocked(cf_id_);
  assert(cf_id_ == cfh->GetID());
  blob_gc_->SetColumnFamily(cfh.get());
  s = HolePunchBlobFiles();
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PunchHoleGCJob::HolePunchBlobFiles() {
  for (const auto& file : blob_gc_->inputs()) {
    if (IsShutingDown()) {
      return Status::ShutdownInProgress();
    }
    Status s = HolePunchSingleBlobFile(file);
    if (!s.ok()) {
      TITAN_LOG_ERROR(db_options_.info_log,
                      "Hole punch file %" PRIu64 " failed: %s",
                      file->file_number(), s.ToString().c_str());

      return s;
    }
  }
  return Status::OK();
}

Status PunchHoleGCJob::HolePunchSingleBlobFile(
    std::shared_ptr<BlobFileMeta> file) {
  Status s;
  auto fd = open(BlobFileName(db_options_.dirname, file->file_number()).c_str(),
                 O_WRONLY);
  std::unique_ptr<RandomAccessFileReader> file_reader;
  s = NewBlobFileReader(file->file_number(), 0, db_options_, env_options_, env_,
                        &file_reader);
  if (!s.ok()) {
    return s;
  }
  uint64_t effective_file_size = 0;
  uint64_t aligned_data_size = 0;
  std::unique_ptr<BlobFileIterator> iter(
      new BlobFileIterator(std::move(file_reader), file->file_number(),
                           file->file_size(), blob_gc_->titan_cf_options()));
  iter->SeekToFirst();
  if (!iter->status().ok()) {
    return iter->status();
  }
  auto block_size = file->block_size();
  for (; iter->Valid(); iter->Next()) {
    if (IsShutingDown()) {
      return Status::ShutdownInProgress();
    }
    BlobIndex blob_index = iter->GetBlobIndex();
    auto key = iter->key();
    bool discardable = false;
    s = WhetherToPunchHole(key, blob_index, &discardable);
    if (!s.ok()) {
      return s;
    }

    aligned_data_size = (blob_index.blob_handle.size + block_size - 1) /
                        block_size * block_size;

    if (!discardable) {
      effective_file_size += aligned_data_size;
      continue;
    }

#if defined(FALLOC_FL_PUNCH_HOLE) && defined(FALLOC_FL_KEEP_SIZE)
    // Hole punch the file at the blob_index.blob_handle.offset with
    // blob_index.blob_handle.size aligned to alignment_size.
    auto err = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                         blob_index.blob_handle.offset, aligned_data_size);
    if (err != 0) {
      return Status::IOError("Hole punch failed", strerror(err));
    }
#else
    return Status::NotSupported("Hole punch not supported");
#endif
  }
  if (!iter->status().ok()) {
    return iter->status();
  }
  effective_file_size_map_[file->file_number()] = effective_file_size;
  return Status::OK();
}

Status PunchHoleGCJob::WhetherToPunchHole(const Slice& key,
                                          const BlobIndex& blob_index,
                                          bool* discardable) {
  // TitanStopWatch sw(env_, metrics_.gc_read_lsm_micros);
  assert(discardable != nullptr);
  PinnableSlice index_entry;
  bool is_blob_index = false;
  DBImpl::GetImplOptions gopts;
  gopts.column_family = blob_gc_->column_family_handle();
  gopts.value = &index_entry;
  gopts.is_blob_index = &is_blob_index;
  auto read_opts = ReadOptions();
  read_opts.snapshot = snapshot_;
  Status s = base_db_impl_->GetImpl(read_opts, key, gopts);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  // count read bytes for checking LSM entry
  // metrics_.gc_bytes_read += key.size() + index_entry.size();
  if (s.IsNotFound() || !is_blob_index) {
    // Either the key is deleted or updated with a newer version which is
    // inlined in LSM.
    *discardable = true;
    return Status::OK();
  }

  BlobIndex other_blob_index;
  s = other_blob_index.DecodeFrom(&index_entry);
  if (!s.ok()) {
    return s;
  }

  *discardable = !(blob_index == other_blob_index);
  return Status::OK();
}

void PunchHoleGCJob::UpdateBlobFilesMeta() {
  for (auto& file : blob_gc_->inputs()) {
    if (file->is_obsolete()) {
      continue;
    }
    auto it = effective_file_size_map_.find(file->file_number());
    if (it == effective_file_size_map_.end()) {
      continue;
    }
    file->set_effective_file_size(it->second);
  }
}

Status PunchHoleGCJob::Cleanup() {
  base_db_impl_->ReleaseSnapshot(snapshot_);
  snapshot_ = nullptr;
  // Release input files.
  blob_gc_->ReleaseGcFiles();
  return Status::OK();
}

}  // namespace titandb
}  // namespace rocksdb