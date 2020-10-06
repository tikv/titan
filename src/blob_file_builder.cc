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

BlobIndices BlobFileBuilder::Add(const BlobRecord& record,
                                 std::unique_ptr<BlobIndex> index) {
  BlobIndices ret;
  if (!ok()) return ret;
  std::string key = record.key.ToString();
  if (builder_state_ == BuilderState::kBuffered) {
    std::string record_str;
    // Encode to take ownership of underlying string.
    record.EncodeTo(&record_str);
    sample_records_.emplace_back(record_str);
    sample_str_len_ += record_str.size();
    cached_indices_.push_back(std::make_pair(key, std::move(index)));
    if (cf_options_.blob_file_compression_options.zstd_max_train_bytes > 0 &&
        sample_str_len_ >=
            cf_options_.blob_file_compression_options.zstd_max_train_bytes) {
      ret = EnterUnbuffered();
    }
  } else if (builder_state_ == BuilderState::kUnbuffered) {
    encoder_.EncodeRecord(record);
    WriteEncoderData(&index->blob_handle);
    ret.push_back(std::make_pair(key, std::move(index)));
  }

  // The keys added into blob files are in order.
  // We do key range checks for both state
  if (smallest_key_.empty()) {
    smallest_key_.assign(record.key.data(), record.key.size());
  }
  assert(cf_options_.comparator->Compare(record.key, Slice(smallest_key_)) >=
         0);
  assert(cf_options_.comparator->Compare(record.key, Slice(largest_key_)) >= 0);
  largest_key_.assign(record.key.data(), record.key.size());

  return ret;
}

BlobIndices BlobFileBuilder::EnterUnbuffered() {
  // Using collected samples to train the compression dictionary
  // Then replay those records in memory, encode them to blob file
  // When above things are done, transform builder state into unbuffered
  std::string samples = "";
  samples.reserve(sample_str_len_);
  std::vector<size_t> sample_lens;

  for (const auto& record_str : sample_records_) {
    size_t copy_len =
        cf_options_.blob_file_compression_options.zstd_max_train_bytes > 0
            ? std::min(cf_options_.blob_file_compression_options
                               .zstd_max_train_bytes -
                           record_str.size(),
                       record_str.size())
            : record_str.size();
    samples.append(record_str, 0, copy_len);
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

  return FlushSampleRecords();
}

BlobIndices BlobFileBuilder::FlushSampleRecords() {
  assert(cached_indices_.size() == sample_records_.size());
  for (size_t i = 0; i < sample_records_.size(); i++) {
    const std::string& record_str = sample_records_[i];
    std::pair<std::string, std::unique_ptr<BlobIndex>>& key_index =
        cached_indices_[i];
    encoder_.EncodeSlice(record_str);
    WriteEncoderData(&key_index.second->blob_handle);
  }
  sample_records_.clear();
  sample_str_len_ = 0;
  BlobIndices ret = std::move(cached_indices_);
  cached_indices_.clear();
  return ret;
}

void BlobFileBuilder::WriteEncoderData(BlobHandle* handle) {
  handle->offset = file_->GetFileSize();
  handle->size = encoder_.GetEncodedSize();
  live_data_size_ += handle->size;

  status_ = file_->Append(encoder_.GetHeader());
  if (ok()) {
    status_ = file_->Append(encoder_.GetRecord());
    num_entries_++;
  }
}

void BlobFileBuilder::WriteRawBlock(const Slice& block, BlockHandle* handle) {
  handle->set_offset(file_->GetFileSize());
  handle->set_size(block.size());
  status_ = file_->Append(block);
  if (ok()) {
    // follow rocksdb's block based table format
    char trailer[kBlockTrailerSize];
    // only compression dictionary and meta index block are written
    // by this method, we use `kNoCompression` as placeholder
    trailer[0] = kNoCompression;
    char* trailer_without_type = trailer + 1;

    // crc32 checksum
    auto crc = crc32c::Value(block.data(), block.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend to cover compression type
    EncodeFixed32(trailer_without_type, crc32c::Mask(crc));
    status_ = file_->Append(Slice(trailer, kBlockTrailerSize));
  }
}

void BlobFileBuilder::WriteCompressionDictBlock(
    MetaIndexBuilder* meta_index_builder, BlockHandle* handle) {
  WriteRawBlock(compression_dict_->GetRawDict(), handle);
  if (ok()) {
    meta_index_builder->Add(kCompressionDictBlock, *handle);
  }
}

BlobIndices BlobFileBuilder::Finish(Status* finish_status) {
  BlobIndices ret;
  if (!ok()) {
    *finish_status = status();
    return ret;
  }

  if (builder_state_ == BuilderState::kBuffered) {
    ret = EnterUnbuffered();
  }

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
  *finish_status = status();
  return ret;
}

void BlobFileBuilder::Abandon() {}

uint64_t BlobFileBuilder::NumEntries() { return num_entries_; }

}  // namespace titandb
}  // namespace rocksdb
