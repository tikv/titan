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
      encoder_(cf_options_.blob_file_compression,
               cf_options.blob_file_compression_options) {
  BlobFileHeader header;
  if (cf_options.blob_file_compression_options.max_dict_bytes > 0)
    header.flags |= BlobFileHeader::kHasUncompressionDictionary;
  std::string buffer;
  header.EncodeTo(&buffer);
  status_ = file_->Append(buffer);
}

void BlobFileBuilder::Add(const BlobRecord& record, BlobHandle* handle) {
  if (!ok()) return;
  if (builder_state_ == BuilderState::kBuffered) {
    std::string rec_str;
    // Encode to take ownership of underlying string.
    record.EncodeTo(&rec_str);
    sample_records_.emplace_back(rec_str);
    sample_str_len_ += rec_str.size();
    if (cf_options_.blob_file_compression_options.zstd_max_train_bytes > 0 &&
        sample_str_len_ >=
            cf_options_.blob_file_compression_options.zstd_max_train_bytes) {
      EnterUnbuffered();
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

  for (const auto& rec_str : sample_records_) {
    size_t copy_len =
        cf_options_.blob_file_compression_options.zstd_max_train_bytes > 0
            ? std::min(cf_options_.blob_file_compression_options
                               .zstd_max_train_bytes -
                           rec_str.size(),
                       rec_str.size())
            : rec_str.size();
    samples.append(rec_str, 0, copy_len);
    sample_lens.emplace_back(copy_len);
  }
  std::string dict;
  dict = ZSTD_TrainDictionary(
      samples, sample_lens,
      cf_options_.blob_file_compression_options.max_dict_bytes);

  compression_dict_.reset(
      new CompressionDict(dict, cf_options_.blob_file_compression,
                          cf_options_.blob_file_compression_options.level));
  encoder_.SetCompressionDict(*compression_dict_);

  builder_state_ = BuilderState::kUnbuffered;

  // add history buffer
  BlobHandle handle;
  for (const std::string& rec_str : sample_records_) {
    BlobRecord rec;
    Slice rec_slice(rec_str);
    rec.DecodeFrom(&rec_slice);
    Add(rec, &handle);
  }
  sample_records_.clear();
  sample_str_len_ = 0;
}

void BlobFileBuilder::WriteRawBlock(const Slice& block, BlockHandle* handle) {
  handle->set_offset(file_->GetFileSize());
  handle->set_size(block.size());
  status_ = file_->Append(block);
}

void BlobFileBuilder::WriteCompressionDictBlock(
    MetaIndexBuilder* meta_index_builder, BlockHandle* handle) {
  WriteRawBlock(compression_dict_->GetRawDict(), handle);
  if (ok()) {
    meta_index_builder->Add(kCompressionDictBlock, *handle);
  }
}

Status BlobFileBuilder::Finish() {
  if (!ok()) return status();

  if (builder_state_ == BuilderState::kBuffered) EnterUnbuffered();

  BlobFileFooter footer;

  // if has compression dictionary, encode it into meta blocks
  // and update relative fields in footer
  if (cf_options_.blob_file_compression_options.max_dict_bytes > 0) {
    BlockHandle meta_index_handle, uncompression_dict_handle;
    MetaIndexBuilder meta_index_builder;
    WriteCompressionDictBlock(&meta_index_builder, &uncompression_dict_handle);
    WriteRawBlock(meta_index_builder.Finish(), &meta_index_handle);
    footer.meta_index_handle = meta_index_handle;
    footer.uncompression_dict_handle = uncompression_dict_handle;
  }

  std::string buffer;
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
