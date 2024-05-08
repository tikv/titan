#include "blob_aligned_blocks_collector.h"

#include "base_db_listener.h"
#include "titan_logging.h"

namespace rocksdb {
namespace titandb {

TablePropertiesCollector*
BlobAlignedBlocksCollectorFactory::CreateTablePropertiesCollector(
    rocksdb::TablePropertiesCollectorFactory::Context /* context */) {
  return new BlobAlignedBlocksCollector(info_logger_);
}

const std::string BlobAlignedBlocksCollector::kPropertiesName =
    "TitanDB.blob_aligned_blocks";

bool BlobAlignedBlocksCollector::Encode(
    const std::map<uint64_t, uint64_t>& aligned_blocks, std::string* result) {
  PutVarint32(result, static_cast<uint32_t>(aligned_blocks.size()));
  for (const auto& f_blocks : aligned_blocks) {
    PutVarint64(result, f_blocks.first);
    PutVarint64(result, f_blocks.second);
  }
  return true;
}
bool BlobAlignedBlocksCollector::Decode(
    Slice* slice, std::map<uint64_t, uint64_t>* aligned_blocks) {
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
    (*aligned_blocks)[file_number] = size;
  }
  return true;
}

Status BlobAlignedBlocksCollector::AddUserKey(const Slice& /* key */,
                                              const Slice& value,
                                              EntryType type,
                                              SequenceNumber /* seq */,
                                              uint64_t /* file_size */) {
  if (type != kEntryBlobIndex) {
    return Status::OK();
  }

  Slice copy = value;

  BlobIndex index;
  auto s = index.DecodeFrom(const_cast<Slice*>(&copy));
  if (!s.ok()) {
    return s;
  }

  auto iter = aligned_blocks_.find(index.file_number);
  if (iter == aligned_blocks_.end()) {
    aligned_blocks_[index.file_number] = index.blob_handle.size / 4096 + 1;
  } else {
    iter->second += index.blob_handle.size / 4096 + 1;
  }

  return Status::OK();
}

Status BlobAlignedBlocksCollector::Finish(UserCollectedProperties* properties) {
  if (aligned_blocks_.empty()) {
    return Status::OK();
  }
  if (info_logger_ != nullptr) {
    TITAN_LOG_INFO(
        info_logger_,
        "BlobAlignedBlocksCollector::Finish: aligned_blocks size %zu",
        aligned_blocks_.size());
  }

  std::string res;
  bool ok __attribute__((__unused__)) = Encode(aligned_blocks_, &res);
  assert(ok);
  assert(!res.empty());
  properties->emplace(std::make_pair(kPropertiesName, res));
  return Status::OK();
}

}  // namespace titandb
}  // namespace rocksdb
