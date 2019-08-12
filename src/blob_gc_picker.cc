#include "blob_gc_picker.h"

namespace rocksdb {
namespace titandb {

BasicBlobGCPicker::BasicBlobGCPicker(TitanDBOptions db_options,
                                     TitanCFOptions cf_options)
    : db_options_(db_options), cf_options_(cf_options) {}

BasicBlobGCPicker::~BasicBlobGCPicker() {}

std::unique_ptr<BlobGC> BasicBlobGCPicker::PickBlobGC(
    BlobStorage* blob_storage) {
  Status s;

  bool stop_picking = false;
  bool maybe_continue_next_time = false;
  auto gc_scores = blob_storage->gc_score();

  // files with small size GC first
  std::sort(gc_scores.begin(), gc_scores.end(),
            [](const GCScore& first, const GCScore& second) {
              return first.gc_score < second.gc_score;
            });

  std::vector<BlobFileMeta*> gc_blob_files;
  uint64_t gc_batch_size = 0;
  uint64_t estimate_output_size = 0;
  uint64_t next_gc_size = 0;
  for (const auto& gc_score : gc_scores) {
    auto blob_file = blob_storage->FindFile(gc_score.file_number).lock();
    if (!blob_file ||
        blob_file->file_state() == BlobFileMeta::FileState::kBeingGC ||
        !CheckBlobFile(blob_file.get())) {
      continue;
    }

    if (gc_score.gc_score > cf_options_.merge_small_file_threshold) {
      break;
    }

    if (gc_batch_size >= cf_options_.max_gc_batch_size ||
        estimate_output_size >= cf_options_.blob_file_target_size) {
      // Stop pick file for this gc, but still check file for whether need
      // trigger gc after this
      stop_picking = true;
    }

    if (!stop_picking) {
      gc_blob_files.push_back(blob_file.get());
      gc_batch_size += blob_file->real_file_size();
      estimate_output_size += blob_file->GetValidSize();
    } else {
      next_gc_size += blob_file->real_file_size();
      if (next_gc_size >= cf_options_.min_gc_batch_size) {
        maybe_continue_next_time = true;
        ROCKS_LOG_INFO(db_options_.info_log,
                       "remain more than %" PRIu64
                       " bytes to be gc and trigger after this gc",
                       next_gc_size);
        break;
      }
    }
  }
  // files with larger discardable size Free Space first
  std::sort(gc_scores.begin(), gc_scores.end(),
            [](const GCScore& first, const GCScore& second) {
              return first.fs_score > second.fs_score;
            });

  std::vector<BlobFileMeta*> fs_blob_files;
  uint64_t next_fs_size = 0;
  uint64_t fs_batch_size = 0;
  stop_picking = false;
  for (const auto& fs_score : gc_scores) {
    // fs_score < 0 means file had over reclaim
    if (fs_score.fs_score < 0) {
      break;
    }

    auto blob_file = blob_storage->FindFile(fs_score.file_number).lock();
    if (!blob_file ||
        blob_file->file_state() == BlobFileMeta::FileState::kBeingGC ||
        !CheckBlobFile(blob_file.get())) {
      continue;
    }

    if (fs_batch_size >= cf_options_.max_fs_batch_size) {
      stop_picking = true;
    }

    if (!stop_picking) {
      fs_blob_files.push_back(blob_file.get());
      fs_batch_size += blob_file->real_file_size();
    } else {
      if (maybe_continue_next_time) {
        break;
      }
      if (blob_file->discardable_size() >=
          static_cast<int64_t>(cf_options_.free_space_threshold)) {
        next_fs_size += blob_file->real_file_size();
        if (next_fs_size >= cf_options_.min_fs_batch_size) {
          maybe_continue_next_time = true;
          ROCKS_LOG_INFO(db_options_.info_log,
                         "remain more than %" PRIu64
                         " bytes to be dig hole and trigger after this gc",
                         next_fs_size);
          break;
        }
      } else {
        break;
      }
    }
  }

  ROCKS_LOG_DEBUG(db_options_.info_log,
                  "got gc batch size %" PRIu64
                  ", got dig hole batch size %" PRIu64 " bytes",
                  gc_batch_size, fs_batch_size);
  if ((gc_blob_files.empty() ||
       gc_batch_size < cf_options_.min_gc_batch_size) &&
      (fs_blob_files.empty() || fs_batch_size < cf_options_.min_fs_batch_size))
    return nullptr;

  return std::unique_ptr<BlobGC>(
      new BlobGC(std::move(gc_blob_files), std::move(fs_blob_files),
                 std::move(cf_options_), maybe_continue_next_time));
}

bool BasicBlobGCPicker::CheckBlobFile(BlobFileMeta* blob_file) const {
  assert(blob_file->file_state() != BlobFileMeta::FileState::kInit);
  if (blob_file->file_state() != BlobFileMeta::FileState::kNormal) return false;

  return true;
}

}  // namespace titandb
}  // namespace rocksdb
