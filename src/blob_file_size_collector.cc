#include "blob_file_size_collector.h"

#include "base_db_listener.h"

namespace rocksdb {
namespace titandb {

TablePropertiesCollector*
BlobFileSizeCollectorFactory::CreateTablePropertiesCollector(
    rocksdb::TablePropertiesCollectorFactory::Context context) {
  if (blob_file_set_ != nullptr) {
    return new BlobFileSizeCollector(
        blob_file_set_->GetBlockSize(context.column_family_id),
        blob_file_set_->GetFileBlockSizes(context.column_family_id));
  }
  return new BlobFileSizeCollector(0, {});
}

const std::string BlobFileSizeCollector::kPropertiesName =
    "TitanDB.blob_discardable_size";

bool BlobFileSizeCollector::Encode(
    const std::map<uint64_t, uint64_t>& blob_files_size, std::string* result) {
  PutVarint32(result, static_cast<uint32_t>(blob_files_size.size()));
  for (const auto& bfs : blob_files_size) {
    PutVarint64(result, bfs.first);
    PutVarint64(result, bfs.second);
  }
  return true;
}
bool BlobFileSizeCollector::Decode(
    Slice* slice, std::map<uint64_t, uint64_t>* blob_files_size) {
  uint32_t num = 0;
  if (!GetVarint32(slice, &num)) {
    return false;
  }
  uint64_t file_number;
  uint64_t size;
  for (uint32_t i = 0; i < num; ++i) {
    if (!GetVarint64(slice, &file_number)) {
      return false;
    }
    if (!GetVarint64(slice, &size)) {
      return false;
    }
    (*blob_files_size)[file_number] = size;
  }
  return true;
}

Status BlobFileSizeCollector::AddUserKey(const Slice& /* key */,
                                         const Slice& value, EntryType type,
                                         SequenceNumber /* seq */,
                                         uint64_t /* file_size */) {
  if (type != kEntryBlobIndex) {
    return Status::OK();
  }

  BlobIndex index;
  auto s = index.DecodeFrom(const_cast<Slice*>(&value));
  if (!s.ok()) {
    return s;
  }

  auto size = index.blob_handle.size;
  if (default_block_size_ > 0 && !file_block_sizes_.empty()) {
    // If the blob file cannot be found in the block size map, it must be a
    // newly created file that has not been added blob_file_set, in this case,
    // we know the block size of the file is default_block_size_.
    // If the blob file can be found in the block size map, it implies we are
    // moving the reference only, while keeping the blob at the original file,
    // in this case, we should use the block size of the original file.
    uint64_t block_size = default_block_size_;
    if (!file_block_sizes_.empty()) {
      auto iter = file_block_sizes_.find(index.file_number);
      if (iter != file_block_sizes_.end()) {
        block_size = iter->second;
      }
    }
    if (block_size > 0) {
      // Align blob size with block size.
      size = (size + block_size - 1) / block_size * block_size;
    }
  }

  auto iter = blob_files_size_.find(index.file_number);
  if (iter == blob_files_size_.end()) {
    blob_files_size_[index.file_number] = size;
  } else {
    iter->second += size;
  }

  return Status::OK();
}

Status BlobFileSizeCollector::Finish(UserCollectedProperties* properties) {
  if (blob_files_size_.empty()) {
    return Status::OK();
  }

  std::string res;
  bool ok __attribute__((__unused__)) = Encode(blob_files_size_, &res);
  assert(ok);
  assert(!res.empty());
  properties->emplace(std::make_pair(kPropertiesName, res));
  return Status::OK();
}

}  // namespace titandb
}  // namespace rocksdb
