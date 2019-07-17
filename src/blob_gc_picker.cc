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
  std::vector<BlobFileMeta*> blob_files;

  uint64_t batch_size = 0;
  uint64_t min_output_threshold =
      std::max(cf_options_.merge_small_file_threshold,
               cf_options_.blob_file_target_size / 4);
  uint64_t estimate_output_size = 0;
  //  ROCKS_LOG_INFO(db_options_.info_log, "blob file num:%lu gc score:%lu",
  //                 blob_storage->NumBlobFiles(),
  //                 blob_storage->gc_score().size());
  bool stop_picking = false;
  bool maybe_continue_next_time = false;
  uint64_t next_gc_size = 0;
  for (auto& gc_score : blob_storage->gc_score()) {
    auto blob_file = blob_storage->FindFile(gc_score.file_number).lock();
    assert(blob_file);
    if (!blob_file ||
        blob_file->file_state() == BlobFileMeta::FileState::kBeingGC) {
      // skip this file id this file is being GC
      // or this file had been GC
      continue;
    }

    //    ROCKS_LOG_INFO(db_options_.info_log,
    //                   "file number:%lu score:%f being_gc:%d pending:%d, "
    //                   "size:%lu discard:%lu mark_for_gc:%d
    //                   mark_for_sample:%d", blob_file->file_number_,
    //                   gc_score.score, blob_file->being_gc,
    //                   blob_file->pending, blob_file->file_size_,
    //                   blob_file->discardable_size_,
    //                   blob_file->marked_for_gc_,
    //                   blob_file->marked_for_sample);

    if (!CheckBlobFile(blob_file.get())) {
      ROCKS_LOG_INFO(db_options_.info_log, "file number:%lu no need gc",
                     blob_file->file_number());
      continue;
    }

    if (!stop_picking) {
      blob_files.push_back(blob_file.get());
      batch_size += blob_file->file_size();
      estimate_output_size +=
          (blob_file->file_size() - blob_file->discardable_size());
      if (batch_size >= cf_options_.max_gc_batch_size &&
          estimate_output_size >= min_output_threshold) {
        // stop pick file for this gc, but still check file for whether need
        // trigger gc after this
        stop_picking = true;
      }
    } else {
      if (blob_file->file_size() <= cf_options_.merge_small_file_threshold ||
          blob_file->gc_mark() ||
          blob_file->GetDiscardableRatio() >=
              cf_options_.blob_file_discardable_ratio) {
        next_gc_size += blob_file->file_size();
        if (next_gc_size > cf_options_.min_gc_batch_size) {
          maybe_continue_next_time = true;
          ROCKS_LOG_INFO(db_options_.info_log,
                         "remain more than %" PRIu64
                         " bytes to be gc and trigger after this gc",
                         next_gc_size);
          break;
        }
      } else {
        break;
      }
    }
  }
  ROCKS_LOG_DEBUG(db_options_.info_log,
                  "got batch size %" PRIu64 ", min output threshold:%" PRIu64
                  ", estimate output %" PRIu64 " bytes",
                  batch_size, min_output_threshold, estimate_output_size);
  if (blob_files.empty() || batch_size < cf_options_.min_gc_batch_size)
    return nullptr;

  return std::unique_ptr<BlobGC>(new BlobGC(
      std::move(blob_files), std::move(cf_options_), maybe_continue_next_time));
}

bool BasicBlobGCPicker::CheckBlobFile(BlobFileMeta* blob_file) const {
  assert(blob_file->file_state() != BlobFileMeta::FileState::kInit);
  if (blob_file->file_state() != BlobFileMeta::FileState::kNormal) return false;

  return true;
}

}  // namespace titandb
}  // namespace rocksdb
