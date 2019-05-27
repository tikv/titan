#include "blob_file_builder.h"

#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

BlobFileBuilder::BlobFileBuilder(const TitanDBOptions& db_options,
                                 const TitanCFOptions& cf_options,
                                 WritableFileWriter* file, TitanStats* stats)
    : cf_options_(cf_options),
      file_(file),
      encoder_(cf_options_.blob_file_compression),
      stats_(stats) {
  BlobFileHeader header;
  std::string buffer;
  header.EncodeTo(&buffer);
  status_ = file_->Append(buffer);
}

void BlobFileBuilder::Add(const BlobRecord& record, BlobHandle* handle) {
  if (!ok()) return;

  encoder_.EncodeRecord(record);
  handle->offset = file_->GetFileSize();
  handle->size = encoder_.GetEncodedSize();

  status_ = file_->Append(encoder_.GetHeader());
  if (ok()) {
    status_ = file_->Append(encoder_.GetRecord());
  }
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
    AddStats(stats_, TitanInternalStats::BLOB_FILE_SIZE, file_->GetFileSize());
  }
  return status();
}

void BlobFileBuilder::Abandon() {}

}  // namespace titandb
}  // namespace rocksdb
