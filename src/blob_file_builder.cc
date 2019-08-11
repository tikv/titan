#include "blob_file_builder.h"

namespace rocksdb {
namespace titandb {

BlobFileBuilder::BlobFileBuilder(const TitanDBOptions& db_options,
                                 const TitanCFOptions& cf_options,
                                 WritableFileWriter* file)
    : cf_options_(cf_options),
      file_(file),
      encoder_(cf_options_.blob_file_compression) {
  BlobFileHeader header;
  std::string buffer;
  header.EncodeTo(&buffer);
  status_ = file_->Append(buffer);
  remain_size_ = block_size_ - (file_->GetFileSize() % block_size_);
  file_->Append(Slice(zero_buffer_, remain_size_));
  remain_size_ = block_size_;
}

void BlobFileBuilder::Add(const BlobRecord& record, BlobHandle* handle) {
  if (!ok()) return;

  encoder_.EncodeRecord(record);
  handle->size = encoder_.GetEncodedSize();
  if (remain_size_ != block_size_ && handle->size > remain_size_) {
    file_->Append(Slice(zero_buffer_, remain_size_));
  }
  handle->offset = file_->GetFileSize();

  status_ = file_->Append(encoder_.GetHeader());
  if (ok()) {
    status_ = file_->Append(encoder_.GetRecord());
  }

  remain_size_ = block_size_ - (file_->GetFileSize() % block_size_);
}

Status BlobFileBuilder::Finish() {
  if (!ok()) return status();

  if (remain_size_ > 0 && remain_size_ < block_size_) {
    file_->Append(Slice(zero_buffer_, remain_size_));
  }

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

}  // namespace titandb
}  // namespace rocksdb
