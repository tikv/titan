#include "table_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "monitoring/statistics.h"

namespace rocksdb {
namespace titandb {

std::unique_ptr<BlobFileBuilder::BlobRecordContext>
TitanTableBuilder::NewCachedRecordContext(const ParsedInternalKey& ikey,
                                          const Slice& value) {
  std::unique_ptr<BlobFileBuilder::BlobRecordContext> ctx(
      new BlobFileBuilder::BlobRecordContext);
  AppendInternalKey(&ctx->key, ikey);
  ctx->has_value = true;
  ctx->value = value.ToString();
  return ctx;
}

void TitanTableBuilder::Add(const Slice& key, const Slice& value) {
  if (!ok()) return;

  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    status_ = Status::Corruption(Slice());
    return;
  }

  uint64_t prev_bytes_read = 0;
  uint64_t prev_bytes_written = 0;
  SavePrevIOBytes(&prev_bytes_read, &prev_bytes_written);

  if (ikey.type == kTypeBlobIndex &&
      cf_options_.blob_run_mode == TitanBlobRunMode::kFallback) {
    // we ingest value from blob file
    Slice copy = value;
    BlobIndex index;
    status_ = index.DecodeFrom(&copy);
    if (!ok()) {
      return;
    }

    BlobRecord record;
    PinnableSlice buffer;
    Status get_status = GetBlobRecord(index, &record, &buffer);
    UpdateIOBytes(prev_bytes_read, prev_bytes_written, &io_bytes_read_,
                  &io_bytes_written_);
    if (get_status.ok()) {
      ikey.type = kTypeValue;
      std::string index_key;
      AppendInternalKey(&index_key, ikey);
      assert(blob_builder_ == nullptr);
      base_builder_->Add(index_key, record.value);
      bytes_read_ += record.size();
    } else {
      // Get blob value can fail if corresponding blob file has been GC-ed
      // deleted. In this case we write the blob index as is to compaction
      // output.
      // TODO: return error if it is indeed an error.
      assert(blob_builder_ == nullptr);
      base_builder_->Add(key, value);
    }
  } else if (ikey.type == kTypeValue &&
             cf_options_.blob_run_mode == TitanBlobRunMode::kNormal) {
    bool is_small_kv = value.size() < cf_options_.min_blob_size;
    if (is_small_kv) {
      if (builder_unbuffered()) {
        // We can append this into SST safely, without disorder issue.
        base_builder_->Add(key, value);
      } else {
        // We have to let builder to cache this KV pair, and it will be returned
        // when state changed
        std::unique_ptr<BlobFileBuilder::BlobRecordContext> ctx =
            NewCachedRecordContext(ikey, value);
        blob_builder_->AddSmall(std::move(ctx));
      }
      return;
    } else {
      // We write to blob file and insert index
      AddBlob(ikey, value);
    }
  } else if (ikey.type == kTypeBlobIndex && cf_options_.level_merge &&
             target_level_ >= merge_level_ &&
             cf_options_.blob_run_mode == TitanBlobRunMode::kNormal) {
    // we merge value to new blob file
    BlobIndex index;
    Slice copy = value;
    status_ = index.DecodeFrom(&copy);
    if (!ok()) {
      return;
    }
    auto storage = blob_storage_.lock();
    assert(storage != nullptr);
    auto blob_file = storage->FindFile(index.file_number).lock();
    if (ShouldMerge(blob_file)) {
      BlobRecord record;
      PinnableSlice buffer;
      Status get_status = GetBlobRecord(index, &record, &buffer);

      // If not ok, write original blob index as compaction output without
      // doing level merge.
      if (get_status.ok()) {
        AddBlob(ikey, record.value);
        if (ok()) return;
      } else {
        ++error_read_cnt_;
        ROCKS_LOG_DEBUG(db_options_.info_log,
                        "Read file %" PRIu64 " error during level merge: %s",
                        index.file_number, get_status.ToString().c_str());
      }
    }
    if (builder_unbuffered()) {
      base_builder_->Add(key, value);
    } else {
      std::unique_ptr<BlobFileBuilder::BlobRecordContext> ctx =
          NewCachedRecordContext(ikey, value);
      blob_builder_->AddSmall(std::move(ctx));
    }
  } else {
    assert(builder_unbuffered());
    base_builder_->Add(key, value);
  }
}

void TitanTableBuilder::AddBlob(const ParsedInternalKey& ikey,
                                const Slice& value) {
  if (!ok()) return;

  BlobRecord record;
  record.key = ikey.user_key;
  record.value = value;

  uint64_t prev_bytes_read = 0;
  uint64_t prev_bytes_written = 0;
  SavePrevIOBytes(&prev_bytes_read, &prev_bytes_written);

  BlobFileBuilder::OutContexts contexts;

  StopWatch write_sw(db_options_.env, statistics(stats_),
                     TITAN_BLOB_FILE_WRITE_MICROS);

  // Init blob_builder_ first
  if (!blob_builder_) {
    // Set the GC's blob file with a high_io pri in ratelimiter
    status_ = blob_manager_->NewFile(&blob_handle_, Env::IOPriority::IO_HIGH);
    if (!ok()) return;
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Titan table builder created new blob file %" PRIu64 ".",
                   blob_handle_->GetNumber());
    blob_builder_.reset(
        new BlobFileBuilder(db_options_, cf_options_, blob_handle_->GetFile()));
  }

  RecordTick(statistics(stats_), TITAN_BLOB_FILE_NUM_KEYS_WRITTEN);
  RecordInHistogram(statistics(stats_), TITAN_KEY_SIZE, record.key.size());
  RecordInHistogram(statistics(stats_), TITAN_VALUE_SIZE, record.value.size());
  AddStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_SIZE,
           record.value.size());
  bytes_written_ += record.key.size() + record.value.size();

  std::unique_ptr<BlobFileBuilder::BlobRecordContext> ctx(
      new BlobFileBuilder::BlobRecordContext);
  AppendInternalKey(&ctx->key, ikey);
  ctx->new_blob_index.file_number = blob_handle_->GetNumber();
  blob_builder_->Add(record, std::move(ctx), &contexts);

  UpdateIOBytes(prev_bytes_read, prev_bytes_written, &io_bytes_read_,
                &io_bytes_written_);

  if (blob_handle_->GetFile()->GetFileSize() >=
      cf_options_.blob_file_target_size) {
    // if blob file hit the size limit, we have to finish it
    // in this case, when calling `BlobFileBuilder::Finish`, builder will be in
    // unbuffered state, so it will not trigger another `AddToBaseTable` call
    FinishBlobFile();
  }

  AddToBaseTable(contexts);
}

void TitanTableBuilder::AddToBaseTable(
    const BlobFileBuilder::OutContexts& contexts) {
  if (contexts.empty()) return;
  for (const std::unique_ptr<BlobFileBuilder::BlobRecordContext>& ctx :
       contexts) {
    ParsedInternalKey ikey;
    if (!ParseInternalKey(ctx->key, &ikey)) {
      status_ = Status::Corruption(Slice());
      return;
    }
    if (ctx->has_value) {
      // write directly to base table
      base_builder_->Add(ctx->key, ctx->value);
    } else {
      RecordTick(statistics(stats_), TITAN_BLOB_FILE_BYTES_WRITTEN,
                 ctx->new_blob_index.blob_handle.size);
      bytes_written_ += ctx->new_blob_index.blob_handle.size;
      if (ok()) {
        std::string index_value;
        ctx->new_blob_index.EncodeTo(&index_value);

        ikey.type = kTypeBlobIndex;
        std::string index_key;
        AppendInternalKey(&index_key, ikey);
        base_builder_->Add(index_key, index_value);
      }
    }
  }
}

void TitanTableBuilder::FinishBlobFile() {
  if (blob_builder_) {
    uint64_t prev_bytes_read = 0;
    uint64_t prev_bytes_written = 0;
    SavePrevIOBytes(&prev_bytes_read, &prev_bytes_written);
    Status s;
    BlobFileBuilder::OutContexts contexts;
    s = blob_builder_->Finish(&contexts);
    UpdateIOBytes(prev_bytes_read, prev_bytes_written, &io_bytes_read_,
                  &io_bytes_written_);
    AddToBaseTable(contexts);

    if (s.ok() && ok()) {
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Titan table builder finish output file %" PRIu64 ".",
                     blob_handle_->GetNumber());
      std::shared_ptr<BlobFileMeta> file = std::make_shared<BlobFileMeta>(
          blob_handle_->GetNumber(), blob_handle_->GetFile()->GetFileSize(),
          blob_builder_->NumEntries(), target_level_,
          blob_builder_->GetSmallestKey(), blob_builder_->GetLargestKey());
      file->FileStateTransit(BlobFileMeta::FileEvent::kFlushOrCompactionOutput);
      finished_blobs_.push_back({file, std::move(blob_handle_)});
      blob_builder_.reset();
    } else {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "Titan table builder finish failed. Delete output file %" PRIu64 ".",
          blob_handle_->GetNumber());
      status_ = blob_manager_->DeleteFile(std::move(blob_handle_));
    }
  }
}

Status TitanTableBuilder::status() const {
  Status s = status_;
  if (s.ok()) {
    s = base_builder_->status();
  }
  if (s.ok() && blob_builder_) {
    s = blob_builder_->status();
  }
  return s;
}

Status TitanTableBuilder::Finish() {
  FinishBlobFile();
  // `FinishBlobFile()` may transform its state from `kBuffered` to
  // `kUnbuffered`, in this case, the relative blob handles will be updated, so
  // `base_builder_->Finish()` have to be after `FinishBlobFile()`
  base_builder_->Finish();
  status_ = blob_manager_->BatchFinishFiles(cf_id_, finished_blobs_);
  if (!status_.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Titan table builder failed on finish: %s",
                    status_.ToString().c_str());
  }
  UpdateInternalOpStats();
  if (error_read_cnt_ > 0) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Read file error %" PRIu64 " times during level merge",
                    error_read_cnt_);
  }
  return status();
}

void TitanTableBuilder::Abandon() {
  base_builder_->Abandon();
  if (blob_builder_) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Titan table builder abandoned. Delete output file %" PRIu64
                   ".",
                   blob_handle_->GetNumber());
    blob_builder_->Abandon();
    status_ = blob_manager_->DeleteFile(std::move(blob_handle_));
  }
}

uint64_t TitanTableBuilder::NumEntries() const {
  if (builder_unbuffered()) {
    return base_builder_->NumEntries();
  } else {
    return blob_builder_->NumEntries() + blob_builder_->NumSampleEntries();
  }
}

uint64_t TitanTableBuilder::FileSize() const {
  return base_builder_->FileSize();
}

bool TitanTableBuilder::NeedCompact() const {
  return base_builder_->NeedCompact();
}

TableProperties TitanTableBuilder::GetTableProperties() const {
  return base_builder_->GetTableProperties();
}

bool TitanTableBuilder::ShouldMerge(
    const std::shared_ptr<rocksdb::titandb::BlobFileMeta>& file) {
  assert(cf_options_.level_merge);
  // Values in blob file should be merged if
  // 1. Corresponding keys are being compacted to last two level from lower
  // level
  // 2. Blob file is marked by GC or range merge
  return file != nullptr &&
         (static_cast<int>(file->file_level()) < target_level_ ||
          file->file_state() == BlobFileMeta::FileState::kToMerge);
}

void TitanTableBuilder::UpdateInternalOpStats() {
  if (stats_ == nullptr) {
    return;
  }
  TitanInternalStats* internal_stats = stats_->internal_stats(cf_id_);
  if (internal_stats == nullptr) {
    return;
  }
  InternalOpType op_type = InternalOpType::COMPACTION;
  if (target_level_ == 0) {
    op_type = InternalOpType::FLUSH;
  }
  InternalOpStats* internal_op_stats =
      internal_stats->GetInternalOpStatsForType(op_type);
  assert(internal_op_stats != nullptr);
  AddStats(internal_op_stats, InternalOpStatsType::COUNT);
  AddStats(internal_op_stats, InternalOpStatsType::BYTES_READ, bytes_read_);
  AddStats(internal_op_stats, InternalOpStatsType::BYTES_WRITTEN,
           bytes_written_);
  AddStats(internal_op_stats, InternalOpStatsType::IO_BYTES_READ,
           io_bytes_read_);
  AddStats(internal_op_stats, InternalOpStatsType::IO_BYTES_WRITTEN,
           io_bytes_written_);
  if (blob_builder_ != nullptr) {
    AddStats(internal_op_stats, InternalOpStatsType::OUTPUT_FILE_NUM);
  }
}

Status TitanTableBuilder::GetBlobRecord(const BlobIndex& index,
                                        BlobRecord* record,
                                        PinnableSlice* buffer) {
  Status s;

  auto it = input_file_prefetchers_.find(index.file_number);
  if (it == input_file_prefetchers_.end()) {
    std::unique_ptr<BlobFilePrefetcher> prefetcher;
    auto storage = blob_storage_.lock();
    assert(storage != nullptr);
    s = storage->NewPrefetcher(index.file_number, &prefetcher);
    if (s.ok()) {
      it = input_file_prefetchers_
               .emplace(index.file_number, std::move(prefetcher))
               .first;
    }
  }

  if (s.ok()) {
    s = it->second->Get(ReadOptions(), index.blob_handle, record, buffer);
  }
  return s;
}

}  // namespace titandb
}  // namespace rocksdb
