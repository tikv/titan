#include "blob_file_builder.h"

namespace rocksdb {
namespace titandb {

BlobFileBuilder::BlobFileBuilder(const TitanDBOptions& db_options,
                                 const TitanCFOptions& cf_options,
                                 WritableFileWriter* file)
    : builder_state_(cf_options.blob_file_compression_options.max_dict_bytes > 0
                         ? BuilderState::kBuffered
                         : BuilderState::kUnbuffered),
      cf_options_(cf_options),
      file_(file),
      encoder_(cf_options_.blob_file_compression) {
  BlobFileHeader header(cf_options.blob_file_compression_options);
  std::string buffer;
  header.EncodeTo(&buffer);
  status_ = file_->Append(buffer);
}

void BlobFileBuilder::Add(const BlobRecord& record, BlobHandle* handle) {
  if (!ok()) return;
  if (builder_state_ == BuilderState::kBuffered) {
    sample_records_.emplace_back(record);
    sample_str_len_ += (16 /* 2 extra Varint64 */ + record.size());
    if (cf_options_.blob_file_compression_options.zstd_max_train_bytes > 0 &&
        sample_str_len_ >=
            cf_options_.blob_file_compression_options.zstd_max_train_bytes) {
      EnterUnbuffered();
      // add history buffer
      for (const BlobRecord& rec : sample_records_) {
        Add(rec, handle);
      }
      sample_records_.clear();
      sample_str_len_ = 0;
    }
    return;
  }

  assert(builder_state_ == BuilderState::kUnbuffered);
  // unbuffered state
  encoder_.EncodeRecord(record);
  handle->offset = file_->GetFileSize();
  handle->size = encoder_.GetEncodedSize();
  live_data_size_ += handle->size;

  status_ = file_->Append(encoder_.GetHeader());
  if (ok()) {
    status_ = file_->Append(encoder_.GetRecord());
    num_entries_++;
    // The keys added into blob files are in order.
    if (smallest_key_.empty()) {
      smallest_key_.assign(record.key.data(), record.key.size());
    }
    assert(cf_options_.comparator->Compare(record.key, Slice(smallest_key_)) >=
           0);
    assert(cf_options_.comparator->Compare(record.key, Slice(largest_key_)) >=
           0);
    largest_key_.assign(record.key.data(), record.key.size());
  }
}

void BlobFileBuilder::EnterUnbuffered() {
  // Using collected samples to train the compression dictionary
  // Then replay those records in memory, encode them to blob file
  // When above things are done, transform builder state into unbuffered
  std::string samples = "";
  std::vector<size_t> sample_lens;
  std::string rec_str;

  const size_t kSampleBytes =
      cf_options_.blob_file_compression_options.zstd_max_train_bytes > 0
          ? cf_options_.blob_file_compression_options.zstd_max_train_bytes
          : cf_options_.blob_file_compression_options.max_dict_bytes;
  for (const auto& rec : sample_records_) {
    rec.EncodeTo(&rec_str);
    size_t copy_len = std::min(kSampleBytes - samples.size(), rec_str.size());
    samples.append(rec_str, 0, copy_len);
    sample_lens.emplace_back(copy_len);
  }
  std::string dict;
  if (cf_options_.blob_file_compression_options.zstd_max_train_bytes > 0) {
    dict = ZSTD_TrainDictionary(
        samples, sample_lens,
        cf_options_.blob_file_compression_options.max_dict_bytes);
  } else {
    dict = std::move(samples);
  }
  CompressionDict compression_dict(
      dict, cf_options_.blob_file_compression,
      cf_options_.blob_file_compression_options.level);
  encoder_.SetCompressionDict(compression_dict);

  builder_state_ = BuilderState::kUnbuffered;
}

Status BlobFileBuilder::Finish() {
  if (!ok()) return status();

  std::string buffer;
  BlobFileFooter footer;
  footer.EncodeTo(&buffer);

  status_ = file_->Append(buffer);
  if (ok()) {
    // The Sync will be done in `BatchFinishFiles`
    status_ = file_->Flush();
  }
  return status();
}

void BlobFileBuilder::Abandon() {}

uint64_t BlobFileBuilder::NumEntries() { return num_entries_; }

}  // namespace titandb
}  // namespace rocksdb
